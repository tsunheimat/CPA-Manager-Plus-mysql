// Package usagepricingsql contains dependency-neutral SQL pricing fragments.
package usagepricingsql

import "fmt"

// ContextIntervalsCTE returns one CTE definition, without WITH or a leading
// comma. Intervals are computed over the small catalog, never over event rows.
// name must be an internal, allowlisted SQL identifier, not a request value.
func ContextIntervalsCTE(name string) string {
	return fmt.Sprintf(`%s as (
	select model, threshold_tokens,
		lead(threshold_tokens) over (partition by model order by threshold_tokens) as next_threshold_tokens
	from model_price_context_tiers
)`, name)
}

// ContextBandJoin returns a leading-space LEFT JOIN classifying each event into
// at most one strict-lower/inclusive-upper context interval. No threshold is
// incremented, including at the int64 boundary. Callers choose their own base
// band for the unmatched row. All arguments must be internal, allowlisted SQL
// identifiers or expressions; request values belong in query bindings.
func ContextBandJoin(intervalName, tierAlias, pricingModel, normalizedInput string) string {
	return fmt.Sprintf(` left join %s %s
		on %s.model = %s
		and %s > %s.threshold_tokens
		and (%s <= %s.next_threshold_tokens or %s.next_threshold_tokens is null)`,
		intervalName, tierAlias, tierAlias, pricingModel, normalizedInput, tierAlias,
		normalizedInput, tierAlias, tierAlias)
}
