## Business Info

This company provides a SaaS data analytics platform used internally and externally. The following 5 pipelines are managed by the Data Engineering team and support either investor reporting or experimentation.

We differentiate pipeline ownership by purpose:
- **Experimentation-focused pipelines** (Unit-level Profit, Daily Growth) are primarily owned by **Data Engineering and Data Science** teams.
- **Investor-focused pipelines** are led by **Finance, Business Analytics, or Engineering**, with **Data Engineering** as a consistent secondary owner for maintenance and data reliability.

---

## Ownership & On-call Rotation Plan

| Pipeline                             | Primary Owner        | Secondary Owner        |
|-------------------------------------|-----------------------|-------------------------|
| Unit-level Profit (Experiments)     | Data Engineering Team | Data Science Team       |
| Aggregate Profit (Investors)        | Finance Team          | Data Engineering Team   |
| Aggregate Growth (Investors)        | Accounts Team         | Data Engineering Team   |
| Daily Growth (Experiments)          | Data Engineering Team | Product Analytics Team  |
| Aggregate Engagement (Investors)    | Software Eng. Team    | Data Engineering Team   |

### On-call Rotation and Transitions

- One **Data Engineer is assigned weekly** to monitor all pipelines.
- On-call schedule rotates among DEs every Monday at 10:00 AM.
- **30-minute handoff sync** every Monday between outgoing and incoming engineers.
- All on-call duties are tracked via Slack channel and JIRA issue board.

### Holiday Coverage

- Engineers must log vacations in a shared team calendar at least 2 weeks in advance.
- If the on-call engineer is on holiday, responsibility shifts:
  1. First to previous week's on-call,
  2. Then to a volunteer/available teammate confirmed in the Monday sync.

### Escalation Procedures

- **Low/Medium Severity**:
  - Logged as a ticket.
  - Handled within 24 hours during business hours.
- **High Severity (data not delivered, pipeline broken)**:
  - Slack `#on-call-alerts` ping + escalation to Slack `#incident-response`
  - If unresolved within 2 hours → alert manager and relevant pipeline owner (e.g., Finance, SWE)

---

## Pipeline Runbooks

---

### 1. Pipeline Name: Unit-level Profit (for Experiments)

1. **Types of data**:
   - Subscription revenue by account
   - Cost per account (support, infra, salaries)
   - Active subscriber counts

2. **Owners**:
   - Primary Owner: Data Engineering Team (design, logic, maintenance)
   - Secondary Owner: Data Science Team (uses data for AB tests)

3. **Common Issues**:
   - Delays in cost attribution (e.g. cloud spend not linked to account)
   - Missing subscriber counts due to upstream sync failure
   - Stale labels in cost system leading to incorrect mappings

4. **SLA’s**:
   - Delivered by 9 AM UTC daily
   - Used by DS team in experiments evaluating profit per feature/account

5. **On-call schedule**:
   - DE team monitors pipeline daily
   - Escalation to DS team if experiment schedule is blocked

---

### 2. Pipeline Name: Aggregate Profit (for Investors)

1. **Types of data**:
   - Revenue from all active accounts
   - Monthly infrastructure and team costs
   - Net profit metrics used in executive reporting

2. **Owners**:
   - Primary Owner: Finance/Risk Team
   - Secondary Owner: Data Engineering Team

3. **Common Issues**:
   - Data mismatch between internal books and automated pipeline
   - Late or missing salary data
   - Errors in aggregation logic (e.g., inactive account counted twice)

4. **SLA’s**:
   - Reviewed monthly by Finance team before investor reporting
   - Final data must be available by the 28th of each month

5. **On-call schedule**:
   - BI team monitors pipeline in final week of each month
   - DE team handles Spark job health, joins, and alert monitoring
   - Escalation to Finance Lead if revenue or cost data is missing/invalid

---

### 3. Pipeline Name: Aggregate Growth (for Investors)

1. **Types of data**:
   - Account creations, cancellations, renewals
   - License upgrades or downgrades
   - Monthly change metrics for investors

2. **Owners**:
   - Primary Owner: Accounts Team
   - Secondary Owner: Data Engineering Team

3. **Common Issues**:
   - Lifecycle events out of sequence (missing change B between A and C)
   - Retention misclassification due to time zone delays
   - Overlapping change logs from multiple CRMs

4. **SLA’s**:
   - Delivered weekly on Fridays
   - Final growth metrics prepared by end of month for investor reporting

5. **On-call schedule**:
   - Monitored during working hours by DE + Accounts Team
   - Issues logged in shared tracker
   - Escalation to Accounts Lead if monthly trends appear inaccurate

---

### 4. Pipeline Name: Daily Growth (for Experiments)

1. **Types of data**:
   - Daily log of subscription activity (signups, downgrades, renewals)
   - Trial conversion and churn rates
   - Growth KPIs for testing feature rollout

2. **Owners**:
   - Primary Owner: Data Engineering Team
   - Secondary Owner: Product Analytics Team

3. **Common Issues**:
   - Trial account conversions missing due to delay in activation system
   - API timeout from subscription provider causes partial log
   - Daily exports processed before source data completes load

4. **SLA’s**:
   - Processed and available daily by 9 AM UTC
   - Used for near-real-time experimentation and product impact metrics

5. **On-call schedule**:
   - Slack alert monitored daily
   - Manual rerun or rollback if freshness threshold not met
   - Escalation to Product Analyst if data quality blocks decision-making

---

### 5. Pipeline Name: Aggregate Engagement (for Investors)

1. **Owners**:
   - Primary Owner: Software Engineering Team
   - Secondary Owner: Data Engineering Team

2. **Types of data**:
   - User activity logs (clicks, time on platform)
   - Aggregated session data per account
   - Metrics on product adoption and stickiness

3. **Common Issues**:
   - Kafka delay → late clickstream
   - Kafka outage → full-day data loss
   - Duplicate events → must be deduplicated in transformation step

4. **SLA’s**:
   - Data should arrive within 48 hours of user activity
   - All investor reporting metrics must be complete by last business day of the month

5. **On-call schedule**:
   - DE team member rotates weekly to monitor aggregation process
   - Coordinated with SWE Kafka team in case of upstream data loss
   - Escalation to Platform PM if engagement metrics cannot be verified

---

## Summary Table

| Pipeline               | SLA                          | Primary Owner        | Common Risks                           |
|------------------------|------------------------------|-----------------------|----------------------------------------|
| Unit-level Profit      | Daily @ 9 AM UTC             | Data Engineering      | Cost attribution errors, subscriber lag|
| Aggregate Profit       | Monthly by 28th              | Finance Team          | File mismatch, salary lag              |
| Aggregate Growth       | Weekly (Fri) + Monthly       | Accounts Team         | Event ordering, CRM sync issues        |
| Daily Growth           | Daily @ 9 AM UTC             | Data Engineering      | Incomplete logs, API delays            |
| Aggregate Engagement   | Within 48 hrs, Monthly report| SWE Team              | Kafka lag, duplication, late joins     |