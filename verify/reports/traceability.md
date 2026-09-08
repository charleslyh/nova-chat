# Coverage detail

**Verdict:** OK — 97/97 (100%)

## Scenarios

- `l1/attempt-fence`: CR-1, CR-3, CR-4, CR-5, CR-7, FR-5, FR-6, INV-1, INV-11, INV-5, INV-6
- `l1/cancel-cross-tenant-denied`: FR-7, SEC-2
- `l1/chain-bytes-limit`: CR-9, FR-17, INV-41, INV-42
- `l1/chain-closure`: CR-12, FR-28, INV-47
- `l1/chain-cross-tenant-denied`: CR-9, INV-41, INV-42, SEC-2, SEC-3
- `l1/chain-delete-head-is-surgical`: CR-10, FR-21, INV-43, INV-46
- `l1/chain-delete-middle-link-blast-radius`: CR-10, FR-18, FR-21, INV-43, INV-46
- `l1/chain-depth-limit`: CR-9, FR-17, INV-41, INV-42
- `l1/chain-multi-turn`: CR-10, CR-4, CR-5, CR-9, FR-15, FR-16, INV-11, INV-41, INV-42, INV-43, INV-46
- `l1/claim-when-empty`: FR-4
- `l1/content-delete-and-sweep`: FR-21, FR-22, OR-5
- `l1/context-store-down-rejects-write`: CR-10, FR-37, INV-46, OR-2
- `l1/double-claim`: CR-1, FR-4, INV-1
- `l1/event-expired-explicit`: CR-10, CR-4, CR-5, FR-12, INV-11, INV-40
- `l1/idempotent-create`: CR-2, CR-4, CR-5, FR-3, INV-11, INV-2
- `l1/inline-binary-rejected`: FR-26, INV-50, SEC-7
- `l1/instructions-not-inherited`: CR-9, FR-19, INV-41, INV-42, INV-49
- `l1/integrity-tamper-detected`: CR-13, FR-39, INV-44
- `l1/internal-url-rejected`: FR-26, SEC-6
- `l1/item-reference-rejected`: FR-25, INV-50, INV-52, SEC-3
- `l1/overload-reject-consistent`: CR-1, CR-2, CR-4, CR-5, FR-33, INV-1, INV-11, INV-2, INV-29, INV-30
- `l1/partial-usage-accounted`: CR-11, CR-6, FR-36, FR-7, INV-35, INV-51
- `l1/pending-limit-overload`: CR-10, CR-2, FR-33, INV-2, INV-29, INV-30, INV-43, INV-46
- `l1/read-only-reject`: CR-10, FR-33, INV-32, INV-43, INV-46
- `l1/reap-closes-lost-claim`: CR-6, FR-35, FR-38, INV-35, INV-45
- `l1/resume-starting-after`: CR-4, CR-5, FR-10, FR-12, FR-9, INV-11, INV-12, INV-40
- `l1/sequential-responses`: CR-10, CR-4, CR-5, CR-6, FR-1, FR-4, FR-8, INV-11, INV-35, INV-43, INV-46
- `l1/store-false-not-referencable`: CR-10, FR-15, FR-18, INV-43
- `l1/tenant-purge`: FR-21, SEC-2
- `l1/unknown-field-rejected`: FR-24, INV-50
- `l2/background-then-subscribe-http`: CR-4, CR-5, FR-10, FR-2, FR-9
- `l2/conversation-transcript-http`: FR-41, FR-45
- `l2/cross-node-stream-http`: FR-11, FR-14, FR-30, FR-31
- `l2/cross-tenant-404-http`: FR-13, SEC-2, SEC-5
- `l2/delete-response-http`: FR-18, FR-21
- `l2/health-and-create`: FR-1, FR-2, FR-29
- `l2/idempotent-create-http`: CR-2, FR-3, INV-2
- `l2/multi-turn-chain-http`: CR-9, FR-16, FR-19
- `l2/pending-limit-http`: FR-33, INV-29, INV-30
- `l2/read-only-http`: FR-33, INV-32, OR-2
- `l2/sync-mode-http`: FR-2, FR-8
- `l2/unknown-field-400-http`: FR-24, FR-27, INV-50
- `l2/z-fr32-no-stickiness-http`: FR-32
- `l2/z-fr34-graceful-drain-http`: FR-34

## Missing baseline

(none)
