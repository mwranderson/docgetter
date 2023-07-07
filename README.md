# docgetter
Slack app that integrates with CostOfCapital project slack to get transcripts.

## usage
Given desired report_number,
accepts command "@docgetter get report [report_number]", where searches for relevant transcripts and returns it.

## Shortcomings as of July 6, 2023
Historical PDF reports are sometimes contained in multiple files. 
From random searches, they seem identical and this seems like a bug in the collection process. 
Currently, solution is random choice --> could benefit from improvement.

