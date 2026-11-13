# Unify the relay prefix key and declare truncatable labels

The `relay_prefix` config key configures the relay prefix template on the
shared relay-prefix surface used across adapters. The default stays `""`
(no prefix).

The renderer declares `sender`, `sender_short`, and `origin_label`
truncatable, so custom `relay_prefix` templates accept `{nameN}` suffixes
that render the first N Unicode code points (N ≥ 1) of these free-text
labels for MeshCore's constrained companion display. Numeric suffixes on
undeclared stems, such as the fixed-width `sender_id` pubkey prefix, pass
through as literals. No automatic prefix is defined for this transport;
the default template remains empty.
