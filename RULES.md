# PFT Agent Dashboard — Complete Rules & Configuration

## Daily Agent Execution
1. **Trigger**: Runs daily, checks Gmail for the morning pending report email
2. **Email source**: `no-reply-report@kapturecrm.com`
3. **Subject match**: "Queue wise pending report last 60 days"
4. **Only first email**: If multiple emails arrive with the same subject, only the FIRST one of the day is used — later ones are IGNORED
5. **Retry logic**: If email hasn't arrived, retry every **5 minutes** until **12:00 PM IST**
6. **Deadline**: If no email by 12:00 PM, stop retrying and log a failure message

## Data Storage Rules
7. **Ticket-level data (full_report_history)**: Keep for **31 days** for dashboard chart filters (queue, L4, bucket). Download-level data (ticket_history): **7 days** only, then auto-delete
8. **Daily summary numbers**: Keep **forever** (infinite retention) — only ~1 KB per day
9. **Router Pickup**: Do NOT store individual ticket rows — only keep the **daily count** in category breakdown
10. **All other categories** (Internet Issues, Refund, Payment Issues, etc.): Store full ticket-level data
11. **New Tickets CSV cache**: Save at processing time, available for download until **11:59 PM** that day, then auto-delete
12. **Database cleanup**: Runs after each daily agent execution

## Dashboard — KPI Summary Cards
13. **7 cards**: Total Pending, Internet Issues, Created on Report Day, Critical (>48h), Partner Queue, CX High Pain, PX-Send to Wiom
14. **Single day view**: Show raw numbers
15. **Multi-day view**: Show aggregation mode bar with options — **Average** (default), Sum, Median, Min, Max, Unique
16. **Unique mode**: Shows deduplicated ticket count + % of total sum
17. **Delta comparison**: Show change vs previous period's average

## Ticket Bifurcation (Category Summary Daily Trend)
18. **% values**: Center-aligned in table cells
19. **Expandable L4 sub-rows**: Click on any L3 category row to expand Disposition Folder Level 4 breakdown
20. **L4 contribution %**: Calculated on the **category total** (not grand total)
21. **Click-to-download**: Clicking any % value downloads a raw CSV of those specific tickets
22. **Filter**: Multi-category filter with checkbox dropdown

## Ticket Aging Breakdown
23. **Separate independent section** (not inside another section)
24. **Display format**: Number on top (bold), % below it (small gray text)
25. **Date range**: Show last 7 days of data including today
26. **Filters**: Same date range + filter options as Category Summary
27. **L3/L4 multi-select**: Checkbox dropdown filters for Disposition Folder Level 3 and Level 4
28. **Combined filtering**: Can select multiple L3 + multiple L4 categories together
29. **TOTAL row updates**: Recalculates based on selected filters
30. **Draggable/movable**: Section can be reordered like other dashboard sections
31. **No distribution column**: Removed the bar chart column

## Removed Sections
32. **Aging Distribution chart** — Removed
33. **Queue Split doughnut chart** — Removed
34. **Queue x Aging Heatmap** — Removed
35. **Daily Trend line chart** — Removed

## Master Sheet Comparison
36. **Snapshot is FIXED** at daily run time — does not change throughout the day
37. **New Tickets CSV**: Cached in database at processing time, always downloadable even if master sheet was manually updated later
38. **Live Upload Status**: "Check Now" button fetches current master sheet state for live comparison
39. **Master sheet URL**: Google Sheets export as CSV
40. **Comparison by**: ticket_no (column A of master sheet)

## Processing Pipeline (Order)
41. Step 1: Search Gmail for today's email
42. Step 2: Download the full pending report (.xlsx)
43. Step 3: Filter Internet Issues tickets - save filtered file
44. Step 4: Save daily snapshot to database (ticket_history + daily_summary)
45. Step 5: Extract category breakdown + save full report (all categories except Router Pickup)
46. Step 6: Fetch master sheet - compare - save snapshot - cache new tickets CSV
47. Step 7: Cleanup old data (full_report: 31-day, ticket_history: 7-day purge + expired cache removal)

## Gmail Configuration
48. **Gmail account**: avakash.gupta@wiom.in
49. **Authentication**: Gmail App Password (stored as GMAIL_APP_PASSWORD env variable)
50. **Protocol**: IMAP (imap.gmail.com:993)

## Aging Calculation
51. **Aging is calculated from**: The time the email/report came (report_time_ist), NOT from current time
52. **Formula**: pending_hours = (report_time_ist - ticket_created_datetime) / 3600
53. **Buckets**: 0-12h, 12-24h, 24-36h, 36-48h, 48-72h, 72-120h, >120h

## Deployment
54. **Platform**: Railway (auto-deploys from GitHub push via `Procfile`)
55. **Entry point**: `python dashboard_server.py`
56. **Database**: SQLite file stored in repo (via Git LFS for files >100 MB)
57. **Local server**: Available via dashboard_server.py for local testing
