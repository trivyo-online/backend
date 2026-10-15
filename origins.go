package main

import "strings"

// normOrigin makes "HTTPS://Site.app/ " and "https://site.app" compare equal.
func normOrigin(o string) string {
	return strings.ToLower(strings.TrimRight(strings.TrimSpace(o), "/"))
}

// parseOrigins splits CORS_ORIGINS on commas or whitespace (so a pasted
// newline is harmless) and normalises each entry.
func parseOrigins(v string) []string {
	var out []string
	for _, f := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\r' || r == '\t' }) {
		if o := normOrigin(f); o != "" {
			out = append(out, o)
		}
	}
	return out
}

// originAllowed matches exact origins, or patterns with one '*', such as
// "https://trivyo-*.vercel.app" for Vercel preview deployments.
func originAllowed(allow []string, origin string) bool {
	o := normOrigin(origin)
	for _, a := range allow {
		if a == o {
			return true
		}
		if pre, suf, ok := strings.Cut(a, "*"); ok && len(o) > len(pre)+len(suf) && strings.HasPrefix(o, pre) && strings.HasSuffix(o, suf) {
			return true
		}
	}
	return false
}
