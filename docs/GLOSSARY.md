# Assignment A glossary

| Term | Meaning |
|---|---|
| **Workflow** | A business process, such as `motor_renewal` or `policy_feedback`. Each workflow has a lead schema and disposition catalogue. |
| **Lead** | A customer record to contact, identified within a workflow by canonical phone number. |
| **Batch** | One uploaded lead file. A lead remains associated with the first batch in which it was accepted. |
| **Lead schema** | Per-workflow definition of accepted fields, aliases, types, required values, defaults, and PII. |
| **PII** | Personally identifiable information. Fields marked as PII must be protected at rest. |
| **Call** | A contact attempt represented by events sharing a `call_id`. A call is counted only when its `call.initiated` event is linked to a lead. |
| **Connectivity rate** | Connected calls divided by total initiated calls. |
| **Disposition** | A classified call outcome, such as `renewed` or `callback_requested`. |
| **Terminal disposition** | A disposition that ends further work on the lead. |
| **Success disposition** | A disposition that counts as a business win for the client. |
| **Override** | A supervisor correction recorded as a new event; the original disposition event is retained. |
| **Interaction ledger** | The append-only record of events. Reports are derived from linked ledger events. |
