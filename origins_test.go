package main

import "testing"

func TestOriginAllowed(t *testing.T) {
	allow := parseOrigins(" https://Frontend-ewg7.vercel.app/ ,\nhttps://frontend-ewg7-*.vercel.app\n")
	cases := map[string]bool{
		"https://frontend-ewg7.vercel.app":                   true,  // trailing slash/case/space in config ignored
		"https://frontend-ewg7-git-main-niranjan.vercel.app": true,  // preview via wildcard
		"https://frontend-ewg7-.vercel.app":                  false, // wildcard needs a non-empty middle
		"https://frontend-ewg7.vercel.app.evil.com":          false, // suffix must match
		"https://evil.com/https://frontend-ewg7.vercel.app":  false,
		"http://frontend-ewg7.vercel.app":                    false, // scheme matters
		"https://other.vercel.app":                           false,
		"":                                                   false,
	}
	for origin, want := range cases {
		if got := originAllowed(allow, origin); got != want {
			t.Errorf("originAllowed(%q) = %v, want %v", origin, got, want)
		}
	}
	if len(allow) != 2 {
		t.Fatalf("parseOrigins: got %v", allow)
	}
}
