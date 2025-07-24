package natsstore

import (
	"regexp"
	"strings"
	"testing"
)

// Simple group extraction function for testing (without full GroupWatcherManager)
func extractGroupFromPath(path []string, regexPatterns []string) (string, bool) {
	if len(regexPatterns) == 0 {
		return "", false
	}

	pathStr := strings.Join(path, "/")
	for _, pattern := range regexPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		matches := re.FindStringSubmatch(pathStr)
		if len(matches) > 1 {
			// Return the first capture group which should be the group identifier
			return matches[1], true
		}
	}

	return "", false
}

func TestGroupExtraction(t *testing.T) {
	tests := []struct {
		name             string
		regexPatterns    []string
		path             []string
		expectedGroup    string
		expectedHasGroup bool
	}{
		{
			name:             "UUID at start of path",
			regexPatterns:    []string{`^([a-f0-9-]{36})/.*`},
			path:             []string{"550e8400-e29b-41d4-a716-446655440000", "data", "schemas"},
			expectedGroup:    "550e8400-e29b-41d4-a716-446655440000",
			expectedHasGroup: true,
		},
		{
			name:             "UUID in middle of path",
			regexPatterns:    []string{`^data/uuid/([a-f0-9-]{36})/.*`},
			path:             []string{"data", "uuid", "550e8400-e29b-41d4-a716-446655440000", "schemas"},
			expectedGroup:    "550e8400-e29b-41d4-a716-446655440000",
			expectedHasGroup: true,
		},
		{
			name:             "No group in path",
			regexPatterns:    []string{`^([a-f0-9-]{36})/.*`},
			path:             []string{"data", "schemas", "user"},
			expectedGroup:    "",
			expectedHasGroup: false,
		},
		{
			name:             "Multiple patterns, second one matches",
			regexPatterns:    []string{`^tenant/([a-f0-9-]{36})/.*`, `^org/([a-f0-9-]{36})/.*`},
			path:             []string{"org", "550e8400-e29b-41d4-a716-446655440000", "data"},
			expectedGroup:    "550e8400-e29b-41d4-a716-446655440000",
			expectedHasGroup: true,
		},
		{
			name:             "Organization ID format",
			regexPatterns:    []string{`^org/([a-zA-Z0-9]{8})/.*`},
			path:             []string{"org", "ACME1234", "data"},
			expectedGroup:    "ACME1234",
			expectedHasGroup: true,
		},
		{
			name:             "User ID format",
			regexPatterns:    []string{`^user/([0-9]+)/.*`},
			path:             []string{"user", "12345", "preferences"},
			expectedGroup:    "12345",
			expectedHasGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group, hasGroup := extractGroupFromPath(tt.path, tt.regexPatterns)

			if hasGroup != tt.expectedHasGroup {
				t.Errorf("Expected hasGroup=%v, got %v", tt.expectedHasGroup, hasGroup)
			}

			if group != tt.expectedGroup {
				t.Errorf("Expected group=%s, got %s", tt.expectedGroup, group)
			}
		})
	}
}

func TestGroupExtractionVariations(t *testing.T) {
	testCases := []struct {
		description string
		pattern     string
		testPaths   map[string]string // path -> expected group (empty if no match)
	}{
		{
			description: "Tenant-based UUID paths",
			pattern:     `^tenant/([a-f0-9-]{36})/.*`,
			testPaths: map[string]string{
				"tenant/550e8400-e29b-41d4-a716-446655440000/data/schemas": "550e8400-e29b-41d4-a716-446655440000",
				"tenant/550e8400-e29b-41d4-a716-446655440000/policies":     "550e8400-e29b-41d4-a716-446655440000",
				"global/data/schemas":      "",
				"tenant/invalid-uuid/data": "",
			},
		},
		{
			description: "Direct UUID at root",
			pattern:     `^([a-f0-9-]{36})/.*`,
			testPaths: map[string]string{
				"550e8400-e29b-41d4-a716-446655440000/data":     "550e8400-e29b-41d4-a716-446655440000",
				"550e8400-e29b-41d4-a716-446655440000/policies": "550e8400-e29b-41d4-a716-446655440000",
				"data/schemas": "",
			},
		},
		{
			description: "Organization-based paths",
			pattern:     `^org/([a-zA-Z0-9]{8})/.*`,
			testPaths: map[string]string{
				"org/ACME1234/data":     "ACME1234",
				"org/COMP5678/policies": "COMP5678",
				"org/invalid123/data":   "",
				"tenant/ACME1234/data":  "",
			},
		},
		{
			description: "Environment-based paths",
			pattern:     `^env/([a-z]+)/.*`,
			testPaths: map[string]string{
				"env/production/config": "production",
				"env/staging/config":    "staging",
				"env/development/data":  "development",
				"environment/prod/data": "",
				"env/PROD/config":       "", // Case sensitive
			},
		},
		{
			description: "User-based paths",
			pattern:     `^user/([0-9]+)/.*`,
			testPaths: map[string]string{
				"user/12345/preferences": "12345",
				"user/67890/settings":    "67890",
				"user/abc123/data":       "",
				"users/12345/data":       "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			for pathStr, expectedGroup := range tc.testPaths {
				// Convert path string to slice
				path := pathStringToSlice(pathStr)

				group, hasGroup := extractGroupFromPath(path, []string{tc.pattern})

				if expectedGroup == "" {
					if hasGroup {
						t.Errorf("Path %s: expected no group, but got %s", pathStr, group)
					}
				} else {
					if !hasGroup {
						t.Errorf("Path %s: expected group %s, but got no group", pathStr, expectedGroup)
					} else if group != expectedGroup {
						t.Errorf("Path %s: expected group %s, got %s", pathStr, expectedGroup, group)
					}
				}
			}
		})
	}
}

// Test single group mode configurations
func TestSingleGroupModeExtraction(t *testing.T) {
	tests := []struct {
		name             string
		singleGroup      string
		singlePattern    string
		suffixRegex      string
		path             []string
		expectedGroup    string
		expectedHasGroup bool
	}{
		{
			name:             "Explicit single group with suffix",
			singleGroup:      "production",
			suffixRegex:      `^data/.*`,
			path:             []string{"data", "schemas", "user"},
			expectedGroup:    "production",
			expectedHasGroup: true,
		},
		{
			name:             "Explicit single group with suffix - no match",
			singleGroup:      "production",
			suffixRegex:      `^data/.*`,
			path:             []string{"system", "health"},
			expectedGroup:    "",
			expectedHasGroup: false,
		},
		{
			name:             "Explicit single group without suffix",
			singleGroup:      "main",
			path:             []string{"anything", "goes"},
			expectedGroup:    "main",
			expectedHasGroup: true,
		},
		{
			name:             "Single pattern with suffix",
			singlePattern:    `^env/([a-z]+)/.*`,
			suffixRegex:      `^env/[a-z]+/config/.*`,
			path:             []string{"env", "production", "config", "database"},
			expectedGroup:    "production",
			expectedHasGroup: true,
		},
		{
			name:             "Single pattern with suffix - pattern matches but suffix doesn't",
			singlePattern:    `^env/([a-z]+)/.*`,
			suffixRegex:      `^env/[a-z]+/config/.*`,
			path:             []string{"env", "production", "data", "users"},
			expectedGroup:    "",
			expectedHasGroup: false,
		},
		{
			name:             "Single pattern without suffix",
			singlePattern:    `^tenant/([a-f0-9-]{36})/.*`,
			path:             []string{"tenant", "550e8400-e29b-41d4-a716-446655440000", "anything"},
			expectedGroup:    "550e8400-e29b-41d4-a716-446655440000",
			expectedHasGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate single group mode extraction
			group, hasGroup := extractGroupSingleMode(tt.path, tt.singleGroup, tt.singlePattern, tt.suffixRegex)

			if hasGroup != tt.expectedHasGroup {
				t.Errorf("Expected hasGroup=%v, got %v", tt.expectedHasGroup, hasGroup)
			}

			if group != tt.expectedGroup {
				t.Errorf("Expected group=%s, got %s", tt.expectedGroup, group)
			}
		})
	}
}

// Helper function to simulate single group mode extraction
func extractGroupSingleMode(path []string, singleGroup, singlePattern, suffixRegex string) (string, bool) {
	pathStr := strings.Join(path, "/")

	if singleGroup != "" {
		// Explicit single group - check suffix regex if configured
		if suffixRegex != "" {
			if matched, _ := regexp.MatchString(suffixRegex, pathStr); matched {
				return singleGroup, true
			}
			return "", false
		}
		// No suffix regex - always use single group
		return singleGroup, true
	}

	if singlePattern != "" {
		// Single group pattern - extract group and check suffix regex
		re, err := regexp.Compile(singlePattern)
		if err != nil {
			return "", false
		}
		matches := re.FindStringSubmatch(pathStr)
		if len(matches) > 1 {
			group := matches[1]
			if suffixRegex != "" {
				if matched, _ := regexp.MatchString(suffixRegex, pathStr); matched {
					return group, true
				}
				return "", false
			}
			return group, true
		}
	}

	return "", false
}

// Helper function to convert path string to slice (for testing)
func pathStringToSlice(pathStr string) []string {
	if pathStr == "" {
		return []string{}
	}

	// Simple split by "/"
	result := []string{}
	current := ""

	for _, char := range pathStr {
		if char == '/' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}

	if current != "" {
		result = append(result, current)
	}

	return result
}

func TestPathStringToSlice(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", []string{}},
		{"tenant", []string{"tenant"}},
		{"tenant/uuid", []string{"tenant", "uuid"}},
		{"tenant/550e8400-e29b-41d4-a716-446655440000/data/schemas",
			[]string{"tenant", "550e8400-e29b-41d4-a716-446655440000", "data", "schemas"}},
		{"env/production/config/database",
			[]string{"env", "production", "config", "database"}},
	}

	for _, tt := range tests {
		result := pathStringToSlice(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("Input %s: expected length %d, got %d", tt.input, len(tt.expected), len(result))
			continue
		}

		for i, expected := range tt.expected {
			if result[i] != expected {
				t.Errorf("Input %s: at index %d expected %s, got %s", tt.input, i, expected, result[i])
			}
		}
	}
}

// Benchmark for group extraction
func BenchmarkGroupExtraction(b *testing.B) {
	testPath := []string{"tenant", "550e8400-e29b-41d4-a716-446655440000", "data", "schemas", "user"}
	patterns := []string{`^tenant/([a-f0-9-]{36})/.*`}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = extractGroupFromPath(testPath, patterns)
	}
}
