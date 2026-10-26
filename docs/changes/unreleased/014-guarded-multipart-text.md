# Opt into complete, paced MeshCore text delivery

`max_text_parts` now permits 2–128 exact numbered packets for long ordinary
messages instead of single-packet clipping; the default remains 1. Attribution
and UTF-8 budgets apply independently to each packet. Exceeding the part cap
rejects the complete message before sending.

The coordinated core PR persists plans and local progress. Each packet receives
a one-use grant inside the existing paced send owner and permits only one native
call. Ambiguous handoffs stop rather than retry, and a partial message cannot
complete the parent or create an arbitrary whole-message native reply target.

Package version stays 0.1.0. Deterministic tests are separate from installed-SDK
and physical-radio qualification; no hardware campaign is claimed here.
