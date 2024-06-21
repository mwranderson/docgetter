# docgetter
Slack app that integrates with CostOfCapital project slack to get transcripts.

## usage
Given desired report_number,
accepts command "@docgetter get report [report_number]", where searches for relevant transcripts and returns it.
<br> Accepts an optional 'transcript_source' number after report_number to uniquely identify desired file. <br>
If multiple files are found and no transcript_source is provided, returns an interactable block for user to select desired report.

## Notes
- Historical PDF reports are sometimes contained in multiple files. 
From random searches, they seem identical and this seems like a bug in the collection process. 
Currently, solution is random choice --> could benefit from improvement.
- CapitalIQ transcript search is very time-inefficient due to different nature of documents. <br>
Since this project runs for free, the code only has enough time to turn some CapitalIQ transcripts into PDFs.<br>
For years past 2009, it is likely manual intervention is required.

