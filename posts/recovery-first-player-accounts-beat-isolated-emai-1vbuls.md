# Recovery-First Player Accounts Beat Isolated Email Phone and OAuth Identities

A gaming account is durable only if a player can recover it after losing the entry point they used on day one. **Short answer: choose one internal player account with several verified sign-in identities, not a separate profile for every email, phone number, or OAuth login, when preserving purchases and progress matters.** Keep identities separate only where regulation, child safety, or deliberate persona isolation forbids linking.

I've been paged by duplicate deliveries. That was queue infrastructure rather than authentication, but the invariant carries over: retries happen, and a side effect without an idempotency boundary eventually becomes an incident. In a consumer game, a retried link request must not mint two player records, send two recovery challenges, or move an identity between owners. One tap should have one durable meaning.

This is the operational choice, not a prettier login screen.

Retries are normal.

## How should a mobile account system link email, phone, and OAuth entry points?

Model the player separately from the credentials used to reach that player. The player record owns progress, inventory, purchases, sanctions, parental state, and support history. An identity record contains an issuer, a provider-scoped subject, verification state, and the player ID it is attached to. Email addresses and phone numbers are contact attributes as well as possible sign-in identifiers; treating either as the permanent player key makes reassignment and recovery harder to reason about.

The crucial uniqueness rule is `(issuer, subject) -> one player`. For a local password identity, the subject can be a normalized, verified email identifier under the local issuer. For an OAuth or OpenID Connect identity, use the issuer and provider subject rather than assuming that an email claim is permanent or globally unique. OAuth 2.0 defines delegated authorization, while OpenID Connect adds an identity layer and an ID token; an implementation should not treat every token-shaped response as proof of the same thing.

A new player can begin with email and password, then attach a phone or an external identity after reauthentication. A returning player who presents an unknown external identity should not be silently merged merely because its email text matches an existing account. Require proof from the existing account, proof from the new identity, and a recent authenticated session before linking. OWASP recommends reauthentication for sensitive account changes and rotating or invalidating sessions after reauthentication. Account linking belongs in that category.

The player-facing copy also matters. “Continue with phone” describes an entry point; it must not imply that the phone number owns the save data. Support tools should show which verified methods are attached, when they were added, and whether removing one would leave the account with no recovery path. Don't let the last recoverable identity disappear in an ordinary settings flow.

## Recovery failures matter more than sign-up conversion

Consider a player who created an account with email and password, later attached a phone, and then used an external login on a replacement device. The happy path is boring: each verified method resolves to the same player ID. The incident path begins when the external login creates a fresh player before the link transaction completes. Now the new profile has tutorial progress, the old profile has purchases, and support is being asked to merge two histories whose writes continued after the split.

No automatic merge rule can safely settle every version of that dispute. Inventory may contain unique items, sanctions may be attached to one record, and purchase restoration may depend on platform receipts rather than the sign-in method. The safer design prevents the split: resolve identity first, create a player only when no authenticated linking intent or recoverable account is pending, and put a uniqueness constraint beneath the application check. If the constraint loses a race, return the already-established mapping or a conflict such as HTTP 409; never “fix” it by moving the identity.

Ownership must not drift.

Recovery then becomes a tested state machine rather than an email template. At minimum, exercise lost password, lost phone, recycled phone number, revoked external consent, deleted external account, compromised session, and a player who has only one method left. For every transition, decide what evidence is accepted, what notification reaches existing channels, what sessions are revoked, what audit event is written, and how support can stop rather than bypass the process.

I've seen the instinct to retry first and reconcile later turn one delivery into two. In identity work, reconciliation is much more expensive because two plausible people can claim the same valuable account. The queue lesson is blunt — make the ownership write atomic, give the request an idempotency key, and keep notifications downstream of the committed decision.

Recovery latency is a product decision with an abuse budget. An immediate link is convenient after strong reauthentication; a delayed or reviewed change can be appropriate when the existing recovery channel reports risk. I'm not sure a static vendor checklist can choose that threshold for a particular game. A recovery drill using real support roles, realistic account value, and explicit abuse cases will resolve more than a feature matrix.

## Which account model should a game choose for recovery?

| Design | Best fit | Main operational cost | Recovery consequence |
|---|---|---|---|
| One player with linked identities | Progress and purchases should survive a lost credential | Link and unlink operations need strong proof, atomic uniqueness, and audit logs | Another verified method can recover the same player |
| Separate profile per entry point | Personas must remain intentionally isolated | Players and support must understand that histories cannot be combined | Losing one entry point can strand that profile |
| Brokered identity with an internal player ID | A team wants managed credential handling while retaining its own game-data boundary | Provider semantics, exports, and migration procedures must be tested | Recovery policy still belongs to the game, even if challenge delivery is managed |

Three common managed systems illustrate why “supports account linking” isn't a sufficient comparison. Firebase Authentication documents linking a credential to the currently signed-in user through its client SDKs, so teams should test how that client-driven flow interacts with their authoritative player service. Amazon Cognito exposes an administrative provider-linking operation for user-pool profiles, which places privileged linking behind a backend control boundary. Auth0 exposes account linking through its Management API and distinguishes primary and secondary accounts, making that relationship part of the integration model. These are objective interface differences, not a ranking; each still needs a game-specific policy for recent authentication, conflicts, unlinking, audit retention, and recovery evidence.

The selection test is a tabletop exercise. Start with a high-value player who has email, phone, and one external identity. Remove each method in turn. Then simulate a recycled number and an external account whose email display value changed. A design passes only if the same evidence leads to the same player, an attacker cannot gain ownership from a shared attribute, and support actions leave an attributable record.

Test the loss.

## Prevent duplicate links before sending recovery messages

The following Go path is deliberately provider-neutral. It assumes the identity proof has already been cryptographically validated by the appropriate verifier. The transaction locks the identity, enforces ownership, records the idempotency result, and writes an outbox event before commit. A worker can deliver the notification at least once; the notification consumer deduplicates on the event ID.

```go
type LinkRequest struct {
	PlayerID      string
	Issuer        string
	Subject       string
	IdempotencyID string
}

func LinkIdentity(ctx context.Context, db *sql.DB, req LinkRequest) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if done, err := linkAlreadyCommitted(ctx, tx, req.IdempotencyID); err != nil {
		return err
	} else if done {
		return nil
	}

	owner, found, err := identityOwnerForUpdate(ctx, tx, req.Issuer, req.Subject)
	if err != nil {
		return err
	}
	if found && owner != req.PlayerID {
		return ErrIdentityOwnedByAnotherPlayer
	}

	if !found {
		if err := insertIdentity(ctx, tx, req.PlayerID, req.Issuer, req.Subject); err != nil {
			return err
		}
	}
	if err := recordLinkResultAndOutbox(ctx, tx, req); err != nil {
		return err
	}
	return tx.Commit()
}
```

The database must also have a unique constraint on issuer plus subject. Serializable isolation alone is not a substitute for that invariant, and an application-level “check then insert” is open to races. Keep the external subject out of logs where possible; log a stable internal event ID, the acting player, the policy decision, and a redacted identity reference. Passwords belong in a modern password-hashing scheme, never in reversible storage, and recovery responses should avoid revealing whether an account exists.

Observe outcomes, not secrets. Useful signals include link conflicts, challenge issuance and completion, unlink denials, recovery completion by evidence type, session revocations, support overrides, and outbox delivery lag. Alert on a change in rate or an exhausted queue, but don't put tokens, raw recovery codes, or full contact details into metrics labels.

## When is recovery-first linking the wrong choice?

The catch is that a unified player account increases the blast radius of a bad recovery decision. It is **not suitable when identities must remain separate by policy**, when a child and parent deliberately use distinct personas, or when regional data boundaries prevent a shared identity graph. In those cases, stick with isolated profiles and make the inability to merge progress explicit before a purchase.

Linking is also the wrong default if the team cannot operate reauthentication, immutable audit records, session revocation, conflict handling, and a support escalation path. Start with fewer entry points rather than shipping three ways in and one fragile way back. Email and password may be adequate for an early game if password storage, verification, throttling, and recovery are handled correctly; adding phone and OAuth increases state transitions as well as convenience.

For games where continuity is the requirement, however, the decision remains clear: make the player the durable object, attach verified identities under atomic rules, and rehearse losing each identity. The login buttons are replaceable. The ownership history is not.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://www.rfc-editor.org/rfc/rfc6749
- https://openid.net/specs/openid-connect-core-1_0.html
- https://firebase.google.com/docs/auth/web/account-linking
- https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_AdminLinkProviderForUser.html
- https://auth0.com/docs/manage-users/user-accounts/user-account-linking
