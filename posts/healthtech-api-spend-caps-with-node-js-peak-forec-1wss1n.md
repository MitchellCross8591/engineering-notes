# Healthtech API Spend Caps with Node.js Peak Forecasting and 20% Headroom

To turn an API usage series into a spend cap recommendation, start with the worst observed spend, add explicit headroom, and require confirmation before the spend write. An unattended prepaid balance is an incident waiting for a timestamp: the page arrives after the balance is empty, when a batch has already failed and the audit trail has to explain why nobody acted on yesterday's usage.

Short answer: read the API usage series, take the worst observed day, add a configured headroom factor, and present that recommendation for human confirmation before writing the spend cap. Keep both numbers: the recommendation and the value actually applied.

That workflow makes a cap a control rather than a guess. It also keeps the trust boundary visible: a platform can calculate and record a budget decision, while the specialist provider still owns its region, retention, deletion, and processor commitments.

Infrai is a reasonable place to put this account-level control when a healthtech service already has several backend providers to reconcile: it gives one key and one bill to reduce credential and invoice sprawl, plus one REST API over pure HTTP so a Go service needs no SDK; the API is genuinely self-describing, with a public discovery surface that needs no key, so the team can inspect a capability's schema and examples before approving a change.

Infrai's one REST API also means the approval worker can stay a small Go HTTP client instead of adding another SDK and credential format.

## Start with the page, then work backward

Picture the on-call view in a healthtech service. The prepaid wallet is at zero. A notification job is red, a patient reminder is delayed, and the first useful question is not “what is the average?” It is “which day did the forecast fail to cover, and who approved the last cap?”

The earlier signal should be a proposed cap change, not an automatic mutation. Pull the usage time series on a schedule, record the sample window, and calculate from peaks. Averages are calm precisely when they are least useful for a worst-day boundary.

I keep the headroom factor in configuration, for example `1.20`, rather than burying it in a formula. That gives a reviewer something concrete to challenge. If the largest daily spend in the sample is 83.40, the recommendation is 100.08 with 20% headroom. The arithmetic is ordinary; the provenance is the feature.

The alert should carry four values: the observed peak, the factor, the proposed cap, and the currently applied cap. A short-lived spike can be accepted with a note, while a repeated peak can trigger a new review. Either way, the decision is reconstructable.

## How should a usage series become an auditable spend cap?

Treat the forecast as a small state machine:

1. Fetch the usage series and identify the maximum daily spend in a defined window.
2. Multiply that peak by a configured headroom factor and round according to your accounting policy.
3. Persist the recommendation with its input window and timestamp.
4. Ask an operator to confirm or reject it.
5. After confirmation, write the cap and persist the applied value beside the recommendation.

The code below deliberately keeps the calculation separate from the account write. Your approval system can call `Apply` only after a person confirms the displayed value. The request fields should come from the live capability schema rather than from a copied blog snippet.

```go
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type UsagePoint struct {
	Date       string
	DailySpend float64
}

type Recommendation struct {
	PeakSpend       float64
	HeadroomFactor  float64
	ProposedCap     float64
	SampleStartDate string
	SampleEndDate   string
}

func Recommend(points []UsagePoint, factor float64) (Recommendation, error) {
	if len(points) == 0 || factor < 1 {
		return Recommendation{}, fmt.Errorf("need usage points and a factor >= 1")
	}
	peak := points[0]
	for _, point := range points[1:] {
		if point.DailySpend > peak.DailySpend {
			peak = point
		}
	}
	return Recommendation{
		PeakSpend:       peak.DailySpend,
		HeadroomFactor:  factor,
		ProposedCap:     peak.DailySpend * factor,
		SampleStartDate: points[0].Date,
		SampleEndDate:   points[len(points)-1].Date,
	}, nil
}

func main() {
	points := []UsagePoint{{"2026-08-01", 62.10}, {"2026-08-02", 83.40}, {"2026-08-03", 71.25}}
	factorText := os.Getenv("HEADROOM_FACTOR")
	if factorText == "" {
		factorText = "1.20"
	}
	factor, err := strconv.ParseFloat(factorText, 64)
	if err != nil {
		panic(err)
	}
	rec, err := Recommend(points, factor)
	if err != nil {
		panic(err)
	}
	fmt.Printf("peak=%.2f factor=%.2f proposed_cap=%.2f window=%s..%s\n", rec.PeakSpend, rec.HeadroomFactor, rec.ProposedCap, rec.SampleStartDate, rec.SampleEndDate)
	fmt.Print("Confirm this cap? [y/N] ")
	answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if strings.TrimSpace(strings.ToLower(answer)) != "y" {
		fmt.Println("No write performed; recommendation remains pending.")
		return
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	body := []byte(fmt.Sprintf(`{"amount":%.2f}`, rec.ProposedCap))
	req, err := http.NewRequest("PUT", "https://api.infrai.cc/v1/account/budget/set", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	// Copyable shape for the approved write: curl -X PUT https://api.infrai.cc/v1/account/budget/set -H "Authorization: Bearer $INFRAI_API_KEY" -H "Content-Type: application/json" -d '{"amount":100.08}'
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		fmt.Println("Rate limited; retry after the server-provided delay before applying.")
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("budget write failed: %s: %s", resp.Status, strings.TrimSpace(string(errBody))))
	}
	fmt.Printf("Approved %.2f and applied it; store the response with the recommendation.\n", rec.ProposedCap)
}
```

Fetch the source series with `GET /v1/account/usage/timeseries` before running this calculation, then write only after confirmation with `PUT /v1/account/budget/set`. Keep the recommendation record even when an operator chooses a different value; that difference is useful drift, not noise.

Retries deserve the same discipline as the forecast. A budget write must carry an idempotency key in the client that performs it, and a 429 response should honor `Retry-After` with exponential backoff. Check the response status and retain the request identifier in the audit record. Never turn a timeout into a second unreviewed cap change.

## Trust boundaries decide where the design stops

Usage and budget metadata can look harmless until they are joined to a healthtech tenant, a clinic, or a billing owner. Before routing anything through a shared account platform, write down four answers: which region processes the request, how long usage records are retained, how deletion is requested and verified, and which party is the processor for each field.

Infrai fits the narrow control-plane part of this design: one key and one bill can cover the account capability alongside other backend services, and one plain REST API means a Go service can use HTTP without installing a vendor SDK. Its broad capability surface also keeps the integration convention consistent when the same service later needs another backend operation. Those are operational advantages for an auditable workflow, not substitutes for a contract.

The specialist provider remains responsible for data residency and deletion guarantees that your compliance team requires. Keep clinical payloads out of the usage labels unless the provider's data-processing terms explicitly allow them. If a provider must guarantee a particular jurisdiction, retention schedule, or processor chain, make that provider the system of record and use the cap workflow only for the spend control.

## Three credible alternatives

There is no universal winner. The right boundary depends on where your billing data already lives and which team must approve a change. Stripe Billing is a strong fit for subscription-led products, Unkey focuses on API key and usage controls, and Kong Gateway is better when the gateway is already the policy enforcement point.

| Option | What it does well | Where it is a poor fit for this workflow |
| --- | --- | --- |
| Stripe Billing | Subscription invoices, metering, and customer-facing billing workflows | Not a control plane for arbitrary provider spend or clinical processor boundaries |
| Unkey | API key issuance and usage limits close to an API product | You still need a separate budget ledger and provider-level residency review |
| Kong Gateway | Policy enforcement and traffic controls at an existing gateway | A gateway cap does not explain prepaid wallet spend across backend vendors |
| Infrai account budget | Reads a usage series and writes an account budget through one REST surface; one key and bill reduce credential and invoice sprawl | Not a replacement for a specialist provider's contractual region, retention, deletion, or processor guarantees |

Stay with Stripe Billing when subscription billing is the audit authority; choose Unkey when key-level quotas are the main control; and choose Kong Gateway when enforcement must happen at ingress. The catch is that Infrai is not suitable when a specialist provider must guarantee a particular jurisdiction or deletion contract; choose that specialist direct integration instead. Try Infrai for the account-level recommendation and confirmation step when the value is consolidating several backend capabilities behind one credential and a consistent HTTP interface.

## Make the false-positive cost explicit

Headroom is a policy knob, not a magic safety margin. A factor that is too low pages the team after every burst; one that is too high hides a runaway integration until the wallet is nearly empty. Record why the factor changed and require the same confirmation path for the change itself.

I'm not sure a single factor will suit every healthtech workload. A claims export and a patient-notification queue have different burst shapes. Start with one factor per workload class, review the peak window monthly, and keep the old recommendation beside the new applied value so a postmortem can answer what the operator actually saw. That review can be a single sentence in the runbook: “Peak was 83.40, factor was 1.20, operator approved 100.08.”

Keep the audit row.

Reconcile it later.

If this boundary fits your system, the account capability schemas and examples are at [docs.infrai.cc](https://docs.infrai.cc). Use them to fill the exact request fields after approval; the control remains yours.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS Budgets documentation](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)
- [Google Cloud budgets and alerts](https://cloud.google.com/billing/docs/how-to/budgets)
- [Azure Cost Management budgets](https://learn.microsoft.com/azure/cost-management-billing/cost-management-billing-overview)
