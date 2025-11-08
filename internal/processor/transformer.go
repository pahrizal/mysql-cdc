package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/dop251/goja"
	"github.com/sirupsen/logrus"

	"mysql-cdc/internal/config"
	"mysql-cdc/internal/models"
)

// ErrEventRejected is returned when a JavaScript transform function rejects an event
// by returning null or undefined
var ErrEventRejected = errors.New("event rejected by transformer")

// Transformer transforms change events based on configuration rules
type Transformer struct {
	config      *config.ProcessorConfig
	logger      *logrus.Logger
	rules       []*RuleMatcher
	jsScript    string // Cached script content
}

// RuleMatcher matches and applies transformation rules
type RuleMatcher struct {
	database   string
	table      string
	include    map[string]bool
	exclude    map[string]bool
	rename     map[string]string
	addFields  map[string]string
}

// NewTransformer creates a new transformer with the given configuration
func NewTransformer(cfg *config.ProcessorConfig, logger *logrus.Logger) (*Transformer, error) {
	if cfg == nil || !cfg.Enabled {
		return &Transformer{
			config: cfg,
			logger: logger,
			rules:  []*RuleMatcher{},
		}, nil
	}

	transformer := &Transformer{
		config: cfg,
		logger: logger,
		rules:  []*RuleMatcher{},
	}

	// Load JavaScript script if specified
	if cfg.Script != "" {
		scriptContent, err := os.ReadFile(cfg.Script)
		if err != nil {
			return nil, fmt.Errorf("failed to read JavaScript script file: %w", err)
		}
		
		// Validate script has transform function
		if err := transformer.validateJavaScriptScript(string(scriptContent)); err != nil {
			return nil, fmt.Errorf("invalid JavaScript script: %w", err)
		}
		
		transformer.jsScript = string(scriptContent)
		logger.Infof("Loaded JavaScript transformation script: %s", cfg.Script)
	}

	// Load YAML-based rules if specified
	if len(cfg.Rules) > 0 {
		rules := make([]*RuleMatcher, 0, len(cfg.Rules))
		for _, rule := range cfg.Rules {
			matcher := &RuleMatcher{
				database:  rule.Database,
				table:     rule.Table,
				include:   make(map[string]bool),
				exclude:   make(map[string]bool),
				rename:    rule.Rename,
				addFields: rule.AddFields,
			}

			// Build include set
			for _, field := range rule.Include {
				matcher.include[strings.ToLower(field)] = true
			}

			// Build exclude set
			for _, field := range rule.Exclude {
				matcher.exclude[strings.ToLower(field)] = true
			}

			rules = append(rules, matcher)
		}
		transformer.rules = rules
	}

	return transformer, nil
}

// validateJavaScriptScript validates that the script exports a transform function
func (t *Transformer) validateJavaScriptScript(scriptContent string) error {
	vm := goja.New()
	
	// Execute the script - it can be:
	// 1. An anonymous function: (function(event) { return event; })
	// 2. A named function: function transform(event) { return event; }
	// 3. A function assigned to a variable: var transform = function(event) { return event; }
	result, err := vm.RunString(scriptContent)
	if err != nil {
		return fmt.Errorf("failed to execute script: %w", err)
	}
	
	// Check if the script result is a function (anonymous function)
	if result != nil && !goja.IsUndefined(result) && !goja.IsNull(result) {
		if _, ok := goja.AssertFunction(result); ok {
			// Script returned an anonymous function - valid
			return nil
		}
	}
	
	// Check if there's a named 'transform' function (backward compatibility)
	transformVar := vm.Get("transform")
	if transformVar != nil && !goja.IsUndefined(transformVar) && !goja.IsNull(transformVar) {
		if _, ok := goja.AssertFunction(transformVar); ok {
			// Named transform function exists - valid
			return nil
		}
	}
	
	return fmt.Errorf("script must export a function (either anonymous function or named 'transform' function)")
}

// Transform applies transformation rules to a change event
func (t *Transformer) Transform(event *models.ChangeEvent) (*models.ChangeEvent, error) {
	// If processor is disabled, return event as-is
	if t.config == nil || !t.config.Enabled {
		return event, nil
	}

	// Use JavaScript script if available (takes precedence over YAML rules)
	if t.jsScript != "" {
		return t.transformWithJavaScript(event)
	}

	// Use YAML-based rules if available
	if len(t.rules) > 0 {
		return t.transformWithRules(event)
	}

	// No transformation configured, return event as-is
	return event, nil
}

// transformWithJavaScript transforms an event using JavaScript script
func (t *Transformer) transformWithJavaScript(event *models.ChangeEvent) (*models.ChangeEvent, error) {
	// Convert event to JSON for JavaScript
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	t.logger.Debugf("Transforming event with JavaScript: %s.%s (type: %s)", event.Database, event.Table, event.Type)

	// Create a new runtime context for this transformation (goja.Runtime is not thread-safe)
	vm := goja.New()

	// Execute the script - support both anonymous functions and named functions
	scriptResult, err := vm.RunString(t.jsScript)
	if err != nil {
		return nil, fmt.Errorf("failed to execute JavaScript script: %w", err)
	}

	var callable goja.Callable
	var ok bool

	// Check if script returned an anonymous function
	if scriptResult != nil && !goja.IsUndefined(scriptResult) && !goja.IsNull(scriptResult) {
		callable, ok = goja.AssertFunction(scriptResult)
		if ok {
			// Anonymous function - use it directly
		}
	}

	// If not anonymous function, check for named 'transform' function (backward compatibility)
	if !ok {
		transformVar := vm.Get("transform")
		if transformVar != nil && !goja.IsUndefined(transformVar) && !goja.IsNull(transformVar) {
			callable, ok = goja.AssertFunction(transformVar)
		}
	}

	if !ok {
		return nil, fmt.Errorf("script must export a function (either anonymous function or named 'transform' function)")
	}

	// Parse event JSON in JavaScript
	if err := vm.Set("eventJSON", string(eventJSON)); err != nil {
		return nil, fmt.Errorf("failed to set event JSON: %w", err)
	}

	parseResult, err := vm.RunString("JSON.parse(eventJSON)")
	if err != nil {
		return nil, fmt.Errorf("failed to parse event JSON: %w", err)
	}
	eventObj := parseResult

	// Call the transform function
	result, err := callable(goja.Undefined(), eventObj)
	if err != nil {
		t.logger.Errorf("JavaScript transform function error: %v", err)
		return nil, fmt.Errorf("JavaScript transform function error: %w", err)
	}

	// Check if result is undefined or null - this means the event should be rejected/dropped
	if result == nil || goja.IsUndefined(result) || goja.IsNull(result) {
		t.logger.Infof("Event rejected by JavaScript transformer: %s.%s (type: %s)", event.Database, event.Table, event.Type)
		return nil, ErrEventRejected
	}

	// Convert result back to Go struct
	exported := result.Export()
	resultJSON, err := json.Marshal(exported)
	if err != nil {
		t.logger.Errorf("Failed to marshal JavaScript result: %v", err)
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}

	t.logger.Debugf("JavaScript transformation result: %s", string(resultJSON))

	// Unmarshal into a map first to preserve all fields
	var resultMap map[string]interface{}
	if err := json.Unmarshal(resultJSON, &resultMap); err != nil {
		t.logger.Errorf("Failed to unmarshal JavaScript result to map: %v, JSON: %s", err, string(resultJSON))
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	// Extract known fields for ChangeEvent struct
	transformed := &models.ChangeEvent{}
	
	if v, ok := resultMap["type"].(string); ok {
		transformed.Type = v
	}
	if v, ok := resultMap["database"].(string); ok {
		transformed.Database = v
	}
	if v, ok := resultMap["table"].(string); ok {
		transformed.Table = v
	}
	if v, ok := resultMap["timestamp"].(float64); ok {
		transformed.Timestamp = int64(v)
	}
	if v, ok := resultMap["rows"].([]interface{}); ok {
		transformed.Rows = make([]map[string]interface{}, 0, len(v))
		for _, row := range v {
			if rowMap, ok := row.(map[string]interface{}); ok {
				transformed.Rows = append(transformed.Rows, rowMap)
			}
		}
	}
	if v, ok := resultMap["old_rows"].([]interface{}); ok {
		transformed.OldRows = make([]map[string]interface{}, 0, len(v))
		for _, row := range v {
			if rowMap, ok := row.(map[string]interface{}); ok {
				transformed.OldRows = append(transformed.OldRows, rowMap)
			}
		}
	}

	// Store the raw JSON to preserve extra fields added by JavaScript
	// The publisher will use this if available
	transformed.RawJSON = resultJSON
	
	t.logger.Debugf("Successfully transformed event: %s.%s", transformed.Database, transformed.Table)
	return transformed, nil
}

// transformWithRules transforms an event using YAML-based rules
func (t *Transformer) transformWithRules(event *models.ChangeEvent) (*models.ChangeEvent, error) {
	// Find matching rule
	var matchedRule *RuleMatcher
	for _, rule := range t.rules {
		if rule.matches(event.Database, event.Table) {
			matchedRule = rule
			break
		}
	}

	// If no rule matches, return event as-is
	if matchedRule == nil {
		return event, nil
	}

	// Create a copy of the event for transformation
	transformed := &models.ChangeEvent{
		Type:      event.Type,
		Database:  event.Database,
		Table:     event.Table,
		Timestamp: event.Timestamp,
		Rows:      make([]map[string]interface{}, 0, len(event.Rows)),
		OldRows:   make([]map[string]interface{}, 0, len(event.OldRows)),
	}

	// Transform rows
	for _, row := range event.Rows {
		transformedRow := t.transformRow(row, matchedRule)
		if transformedRow != nil {
			transformed.Rows = append(transformed.Rows, transformedRow)
		}
	}

	// Transform old rows (for UPDATE events)
	for _, oldRow := range event.OldRows {
		transformedOldRow := t.transformRow(oldRow, matchedRule)
		if transformedOldRow != nil {
			transformed.OldRows = append(transformed.OldRows, transformedOldRow)
		}
	}

	return transformed, nil
}

// transformRow applies transformation rules to a single row
func (t *Transformer) transformRow(row map[string]interface{}, rule *RuleMatcher) map[string]interface{} {
	if row == nil {
		return nil
	}

	transformed := make(map[string]interface{})

	// Add static fields first
	for key, value := range rule.addFields {
		transformed[key] = value
	}

	// Process each field in the row
	for key, value := range row {
		keyLower := strings.ToLower(key)

		// Check if field should be excluded
		if len(rule.exclude) > 0 && rule.exclude[keyLower] {
			continue
		}

		// Check if field should be included (if include list is specified)
		if len(rule.include) > 0 && !rule.include[keyLower] {
			continue
		}

		// Determine the output key name (rename if specified)
		outputKey := key
		if newName, ok := rule.rename[keyLower]; ok {
			outputKey = newName
		}

		transformed[outputKey] = value
	}

	return transformed
}

// matches checks if a rule matches the given database and table
func (r *RuleMatcher) matches(database, table string) bool {
	// Match database (empty = all databases)
	if r.database != "" && !strings.EqualFold(r.database, database) {
		return false
	}

	// Match table (empty = all tables)
	if r.table != "" && !strings.EqualFold(r.table, table) {
		return false
	}

	return true
}

// ValidateRules validates processor configuration rules
func ValidateRules(cfg *config.ProcessorConfig) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}

	// Validate JavaScript script file exists if specified
	if cfg.Script != "" {
		if _, err := os.Stat(cfg.Script); os.IsNotExist(err) {
			return fmt.Errorf("JavaScript script file not found: %s", cfg.Script)
		}
	}

	// Validate that both script and rules are not specified (script takes precedence)
	if cfg.Script != "" && len(cfg.Rules) > 0 {
		return fmt.Errorf("cannot specify both 'script' and 'rules' - script takes precedence")
	}

	for i, rule := range cfg.Rules {
		// Validate that include and exclude are not both specified
		if len(rule.Include) > 0 && len(rule.Exclude) > 0 {
			return fmt.Errorf("processor rule %d: cannot specify both 'include' and 'exclude' fields", i)
		}

		// Validate rename keys exist in include list if include is specified
		// If exclude is specified, rename can be used for any field not in exclude
		// If neither is specified, rename can be used for any field
		if len(rule.Rename) > 0 && len(rule.Include) > 0 {
			for oldName := range rule.Rename {
				found := false
				for _, inc := range rule.Include {
					if strings.EqualFold(inc, oldName) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("processor rule %d: rename key '%s' not found in include list", i, oldName)
				}
			}
		}
	}

	return nil
}

