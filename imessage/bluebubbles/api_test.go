package bluebubbles

import "testing"

// The property that matters here is not "short codes get rewritten" but "chats
// that send fine today are not touched". macOS 26 gives EVERY chat the "any;-;"
// prefix, so a rewrite rule that is one character too greedy moves working
// E.164 and email sends off BlueBubbles' chat-ID path and onto the AppleScript
// fallback. Those cases are the reason this test exists; the two rewrites are
// the easy half.
func TestResolveTahoeAnyGUID(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{
			name: "business chat is iMessage-only",
			in:   "any;-;urn:biz:b15ed773-9eed-11e7-baa2-7b88b04daa8e",
			want: "iMessage;-;urn:biz:b15ed773-9eed-11e7-baa2-7b88b04daa8e",
			ok:   true,
		},
		{
			name: "short code is SMS-only",
			in:   "any;-;83372",
			want: "SMS;-;83372",
			ok:   true,
		},
		// Everything below must come back untouched.
		{name: "E.164 keeps the chat-ID path", in: "any;-;+14155697976"},
		{name: "email keeps the chat-ID path", in: "any;-;peterlvilim@gmail.com"},
		{name: "group chat is not a DM prefix", in: "any;+;chat44483777562009426"},
		{name: "already explicit iMessage", in: "iMessage;-;+14155697976"},
		{name: "already explicit SMS", in: "SMS;-;83372"},
		{name: "empty address", in: "any;-;"},
		{name: "not a GUID", in: "83372"},
		// A leading "+" means E.164, never a short code, even though the rest is
		// digits -- this is the case isShortCode exists to exclude.
		{name: "plus-prefixed digits are E.164", in: "any;-;+18005551212"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveTahoeAnyGUID(tc.in)
			if ok != tc.ok {
				t.Fatalf("resolveTahoeAnyGUID(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("resolveTahoeAnyGUID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
