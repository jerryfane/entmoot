# Entmoot Workshop Submission Summary

Title:
Entmoot: Social Group Communication for Pairwise-Trust Agent Networks

Preferred target:
CoNEXT-style decentralization workshop; use the CoNEXT 2024 DIN workshop as
the precedent, but wait for the accepted CoNEXT 2026 workshop CFP before naming
a specific 2026 workshop.

Short abstract:
Agent systems increasingly rely on pairwise trust, tool protocols, and local
runtime state, but they lack a durable group-communication substrate that can
preserve shared conversation history across partitions and heterogeneous
deployments. Entmoot provides such a layer over Pilot-style encrypted unicast
tunnels: signed group rosters and policies, trust-gated gossip dissemination,
Merkle/RBSR anti-entropy repair, local ESP integration, searchable history, and
agent-facing packaging. A five-day evaluation across four heterogeneous nodes
shows convergence after a residential-edge outage: the final controlled-group
state matched across all participants, with zero missing controlled message IDs
in the generated completeness tables. Additional lab benchmarks exercise
first-broadcast delivery, duplicate-delivery ratio, RBSR bandwidth scaling, and
join-triggered catch-up, while adversarial tests cover non-member connection
rejection, replay rejection, and over-quota stream handling. The result is a
bounded systems claim: signed, repairable group communication for small
pairwise-trust agent overlays is practical today, while Internet-scale
deployment, public artifact release, and behavioral transcript analysis remain
future work.

Keywords:
agent communication, decentralization, gossip protocols, group messaging,
anti-entropy repair, pairwise trust, distributed systems

Fit statement:
The paper fits a decentralization workshop because it studies a deployed
alternative to centralized agent chat and coordination infrastructure. Entmoot
does not replace the underlying unicast trust graph; it adds a group layer that
keeps membership, policy, history, and repair local to the participants. This
matches the CoNEXT/DIN line of work on decentralizing Internet applications and
control points while retaining a concrete systems implementation and measured
evidence.
