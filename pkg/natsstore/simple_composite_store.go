package natsstore

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
)

// pathToSlice converts a storage.Path to a string slice.
func pathToSlice(path storage.Path) []string {
	result := make([]string, len(path))
	for i, segment := range path {
		result[i] = fmt.Sprintf("%v", segment)
	}
	return result
}

// SimpleCompositeStore uses struct embedding and only overrides Read/Write for NATS caching.
type SimpleCompositeStore struct {
	storage.Store // Embedded - automatically inherits all methods
	natsCache     *NATSCache
	logger        logging.Logger

	// Regex patterns for paths that should be handled by NATS K/V cache
	handledPathRegex []*regexp.Regexp

	// Group regex pattern to extract group from paths (for bucket routing)
	groupRegex *regexp.Regexp

	// Single group mode (group is always watched)
	singleGroup string
}

// NewSimpleCompositeStore creates a new simple composite store.
func NewSimpleCompositeStore(originalStore storage.Store, natsCache *NATSCache, logger logging.Logger, handledPathsRegex []string, groupRegexPattern string, singleGroup string) *SimpleCompositeStore {
	var regexps []*regexp.Regexp
	for _, pattern := range handledPathsRegex {
		if re, err := regexp.Compile(pattern); err == nil {
			regexps = append(regexps, re)
		} else {
			logger.Warn("Invalid regex pattern in handled_paths_regex: %s, error: %v", pattern, err)
		}
	}

	// Compile group regex pattern if provided
	var groupRegex *regexp.Regexp
	if groupRegexPattern != "" {
		if re, err := regexp.Compile(groupRegexPattern); err == nil {
			groupRegex = re
		} else {
			logger.Warn("Invalid group regex pattern: %s, error: %v", groupRegexPattern, err)
		}
	}

	return &SimpleCompositeStore{
		Store:            originalStore,
		natsCache:        natsCache,
		logger:           logger,
		handledPathRegex: regexps,
		groupRegex:       groupRegex,
		singleGroup:      singleGroup,
	}
}

func (scs *SimpleCompositeStore) Start(ctx context.Context) error {
	return scs.natsCache.Start(ctx)
}

func (scs *SimpleCompositeStore) Stop(ctx context.Context) error {
	return scs.natsCache.Stop(ctx)
}

// shouldUseNATS determines if a given path should be handled by the NATS K/V cache.
// Two modes:
// 1. Group Pattern: if path matches {group_pattern} AND {any of handledPathRegex}
// 2. Group Absolute Key: if SingleGroup is set AND path matches {any of handledPathRegex}
func (scs *SimpleCompositeStore) shouldUseNATS(path storage.Path) bool {
	pathStr := strings.Join(pathToSlice(path), "/")

	// First check if path matches any handled path regex
	pathMatchesHandledRegex := false
	for _, re := range scs.handledPathRegex {
		if re.MatchString(pathStr) {
			pathMatchesHandledRegex = true
			break
		}
	}

	if !pathMatchesHandledRegex {
		scs.logger.Debug("Path %s does not match any handled path regex, routing to embedded store", pathStr)
		return false
	}

	// Mode 1: Group Pattern - extract group from path
	if scs.groupRegex != nil {
		matches := scs.groupRegex.FindStringSubmatch(pathStr)
		if len(matches) > 1 {
			group := matches[1]
			scs.logger.Debug("Group '%s' extracted from path %s and matches handled path regex, routing to NATS", group, pathStr)
			return true
		}
	}

	// Mode 2: Single Group - group is always watched
	if scs.singleGroup != "" {
		scs.logger.Debug("Single group '%s' mode: path %s matches handled path regex, routing to NATS", scs.singleGroup, pathStr)
		return true
	}

	scs.logger.Debug("Path %s matches handled path regex but no group pattern/single group configured, routing to embedded store", pathStr)
	return false
}

// Read overrides the embedded store's Read method to use NATS cache for specific paths.
func (scs *SimpleCompositeStore) Read(ctx context.Context, txn storage.Transaction, path storage.Path) (interface{}, error) {
	if scs.shouldUseNATS(path) {
		value, found, err := scs.natsCache.Get(ctx, path)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, &storage.Error{Code: storage.NotFoundErr, Message: "path not found"}
		}
		return value, nil
	}

	// Use the embedded store for all other paths
	return scs.Store.Read(ctx, txn, path)
}

// Write overrides the embedded store's Write method to use NATS cache for specific paths.
func (scs *SimpleCompositeStore) Write(ctx context.Context, txn storage.Transaction, op storage.PatchOp, path storage.Path, value interface{}) error {
	if scs.shouldUseNATS(path) {
		if op == storage.RemoveOp {
			return scs.natsCache.Delete(ctx, path)
		}
		return scs.natsCache.Set(ctx, path, value)
	}

	// Use the embedded store for all other paths
	return scs.Store.Write(ctx, txn, op, path, value)
}

// All other methods (NewTransaction, Commit, Abort, Register, Truncate, ListPolicies,
// GetPolicy, UpsertPolicy, DeletePolicy) are automatically inherited from the embedded
// storage.Store and work normally. Only Read and Write need custom routing logic.
