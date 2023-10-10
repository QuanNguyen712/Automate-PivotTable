# Automate-PivotTable

**Motivation:**

ShopeeFood records around 2 million discussion buzzes across the online platform (Facebook, TikTok, news sites etc.), in addition to its competitors’ amount (GoFood, Grab, Baemin, BeFood) which adds up to the total of around 4 million rows of data to worked with every week.


In order to derive metrics for the firm’s weekly report, SQL GROUP BY or Excel PivotTable seem to be underperformed due to their limitation in processing that much data row and especially, the long- time consuming, which costs hours or even a day to accomplish.
The given Python code does the job effortlessly with all manual tasks from data evaluating, merging tables, filtering and applying aggregation to returns the precise, well- structured, ready-to-use output in less than 15 minutes from the run.

**Note:**

The renowned Pandas DataFrame also meets the ‘MemoryError’ in loading this data size; Thus, the Vaex library is apply to tackle this shortcoming.

**How does it work?**

The goal is to come up with (1) a xlsx. file containing all required metric tables to fit in charts/ tables in the given report template. Moreover, tables are stored in worksheets which are equivalent to the respective slides in the template (2).


Check out the report template and sample input/ output data for fully understanding.