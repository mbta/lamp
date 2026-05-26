# goal
We are designing a bus prediction analyzer by inputing gtfs trip_updates and gtfs vehicle_positions. 

- surface low hanging, easily identified as "impossible" predictions
- calculate our own IBI metric on the data for a day/week/month and compare to the vendor provided number
- clear, explainable plots showing specific examples of prediction that are poorly behaved 

# background
IBI metric: https://github.com/TransitApp/ETA-Accuracy-Benchmark

# setup
- config file to tune everything - pass this file around to everything

# backend (polars dataframes, python dataclasses for data. functional programming - no OOP, few classes other than for data organizing)
- the input data is trip_updates and gtfs_vehicle_positions, but the source data for this analyzer should be any arbitrary dataset with the required subset of columns. define these columns in the config. 
- tripupdates add column for ibi bins
- config file can set ignore thresholds - ignore data from 
- configurable ibi bins - configure with a json file - load config from file
- function: calculates % on time based on IBI bins
- bin by arbitrary
- configurable metric to output in the report
- a runner that calculates any number of the selected metrics for any arbitrary grouping of the source data - outputs a dataframe parquet file named according to the filters used. 
- metric to calculate how consistently wrong a trips predictions were. a metric where a consistent "prediction too late" offset demonstrates that the underlying system is not adjusting to the incoming data over the course of a whole trip. detect consistent bias. 

# frontend (this is marimo UI components, plotly express)
- plot: error vs time away
- plot: error vs time away with lines for IBI bins
- check box to draw IBI bins or not - draw IBI bins also populates the % ontime based on IBI bin
- gui with calendar select element that sets service_date - otherwise default to config specified range (last 24 hours). 
- gui elements with text boxes to input route id, trip_id, or stop_id - inputing any of these will refilter the plot and only plot the filtered data. 
- checkbox to bin by time of day - morning rush, mid afternoon, night rush, and late night. configurable spans in the config file

# analysis
- calculate outliers by route - generally worst performing IBI route
- calculate outliers by route/time_of_day
- calculate outlier trips

# ignore this - scratch notes
Do the basic stuff and then various bins bin by arrival time
Do predictions for a single stop generally get more accurate as it gets closer
What are the outliers grouped by route?
What are the outliers groups by time of day?
Which route or trips are consistently miss predicted throughout its entire journey
That’s interesting because it shows that they’re not adjusting it
* To the bins, are you assume they’re doing the IBI bins?