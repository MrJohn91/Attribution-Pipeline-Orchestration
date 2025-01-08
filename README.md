# Attribution-Pipeline-Orchestration

<img src="https://github.com/MrJohn91/Attribution-Pipeline-Orchestration/blob/main/images%3AScreenshots/Diagram.png" height="400" width="800"/>

## Overview

This project is designed to build and implement a data pipeline that automates the attribution modeling process for marketing performance analysis. The primary goal is to calculate attribution weights for different marketing channels and phases of the customer journey (Initializer, Holder, Closer) using the IHC (Initializer, Holder, Closer) model. The system processes data related to user sessions, marketing campaigns, and conversions, applying machine learning-based attribution techniques to determine which marketing channels and phases contribute to the conversion event. The insights provided by the pipeline help understand marketing performance and ROI (Return on Investment)

### Technologies Used

- **Python**: Used for data manipulation, API interaction, and automation tasks.
- **SQLite**: For storing and querying the data.
- **Airflow** Orchestrates the pipeline, allowing tasks to run automatically on new data. It ensures that the pipeline processes new data as it arrives and runs the necessary transformations and reporting steps without manual intervention.
- **External Attribution API**: For computing the attribution based on the customer journey.
- **Data Visualization**: Tools like plotly are used to create visualizations for reporting.

---

## Pipeline Design

### 1. Query Data from the Database
The first step is to retrieve raw data from the **SQLite** database. The relevant tables are:
- `session_sources`: Contains session data including user, session date/time, and channel information.
- `conversions`: Contains conversion date, conversions time and Revenue.
- `session_costs`: Contains the marketing costs associated with each session.

### 2. Transform Data
The data is then transformed into a suitable format for attribution:
- **Filtering sessions**: We filter the sessions based on conversion timestamps, ensuring that only sessions that occurred before the conversion are included.
- **Attribute calculations**: Using the **IHC Attribution Model**, we calculate attribution values for each session in the customer journey. This includes assigning scores to each session based on whether it was part of the **initializer**, **holder**, or **closer** phase of the journey.
- **Metrics Calculation**: We calculate key metrics such as **CPO (Cost Per Order)** and **ROAS (Return on Ad Spend)** for each marketing channel.

### 3. Send Transformed Data to the IHC API
After transforming the data, it is sent to the **IHC Attribution API**, which computes the attribution results (IHC scores) for each session. The result is an attribution value indicating the contribution of each channel at different stages of the customer journey.

### 4. Write Attribution Results to the Database
Once the IHC attribution results are returned, we store these results back in the database:
- **`attribution_customer_journey`** table: Contains the IHC scores for each session.
- **`channel_reporting`** table: Stores aggregated metrics such as **CPO** and **ROAS** by channel for further analysis.

### 5. Query and Export Data from the Database
Finally, the processed data is queried and exported into a CSV file. This file includes the results of the attribution analysis, including detailed channel_reporting data, which can be used for reporting or visualization.

## Airflow Integration
To ensure that the pipeline runs regularly and processes only new data:
- **Airflow** is set up to automate the execution of this pipeline. Once the pipeline is set up, **Airflow** will handle scheduling and running the pipeline at defined intervals (daily, weekly, etc.).
- The task flow is defined in **Airflow** to ensure that each step (querying, transforming, sending to the API, writing results) happens in the right order.
- **Airflow** also ensures that only new data (since the last processed date) is considered for each run, preventing redundant processing of old data.

### Airflow Tasks:
- **Extract Data**: Query new data from the database.
- **Transform Data**: Apply necessary transformations, including data filtering and aggregation.
- **Call API**: Send data to the IHC Attribution API and receive results.
- **Store Results**: Write the attribution results back into the database.
- **Export Data**: Export the results into a CSV for visualization or reporting.

### Airflow Scheduling:
- After implementing the initial pipeline, Airflow will ensure that the process runs on a scheduled basis, checking for new data and updating the results automatically when new data is available in the database.
- 
## Assumptions

1. **Marketing Channel Costs**:
    - It is assumed that **organic traffic** and **direct traffic** do not have associated ad spend, so they are excluded from ROAS calculations. Other channels like paid search or social ads are expected to have ad spend associated with them.

2. **Handling Missing Data**:
    - Missing data, particularly for costs, is handled by filling empty fields with default values (e.g., a cost of `0.0` for missing cost entries in the `session_costs` table). This ensures the pipeline continues to run without data errors.


3. **Scheduling and Automation**:
    - The pipeline is scheduled to run daily to capture new data and generate reports. However, additional flexibility for triggering based on events or data availability could be added for real-time updates.


## Challenges

1. **Merging Data**:
    - Merging session and conversion data required careful handling of timestamps and session IDs to ensure that only relevant sessions were included in the analysis. This was particularly important to ensure that sessions occurring before conversions were considered in the customer journey.

2. **API Integration**: Managing API requests and responses for attribution and handling large amounts of data while maintaining performance.

3. **GitHub Push Issues**:
    - Encountered issues when trying to push changes to GitHub, particularly due to conflicts between local and remote repositories. These issues were resolved by pulling the remote changes, resolving merge conflicts, and carefully pushing the final version of the project to the remote repository.

## Improvements

1. **Scalability**:
    - The current pipeline works well for a moderate amount of data. To handle larger datasets, it could be scaled by integrating cloud storage solutions like Azure Data Lake Storage or Azure Blob Storage, or by using distributed processing frameworks like Apache Spark with Databricks.

2. **Performance Optimization**:
    - The pipeline can be optimized by reducing redundant data transformations and improving the chunking mechanism to handle larger volumes of data more efficiently.

3. **Real-Time Processing**:
    - The pipeline currently can run on a fixed schedule. It could be enhanced to support real-time data processing, where the pipeline is triggered automatically by the arrival of new data or specific events, without requiring manual intervention.


## Visualizations

### 1. **IHC Attribution Results**
[View Visualization](https://github.com/MrJohn91/Attribution-Pipeline-Orchestration/blob/main/images%3AScreenshots/Attribution%20Result.png)
- **Insight**: This chart shows the fraction of **IHC Attribution** received by each channel. **Direct Traffic** shows significant attribution despite having no associated ad spend, indicating its importance in the user journey.

### 2. **IHC Scores**
[View Visualization](https://github.com/MrJohn91/Attribution-Pipeline-Orchestration/blob/main/images%3AScreenshots/IHC%20scores.png)
- **Insight**: The chart visualizes **Initializer**, **Holder**, and **Closer** scores, showing that **Direct Traffic** plays a crucial role in the early stages (initializer) of the customer journey.

### 3. **ROAS by Channel**
[View Visualization](https://github.com/MrJohn91/Attribution-Pipeline-Orchestration/blob/main/images%3AScreenshots/ROAS%20by%20Channel.png)
- **Insight**: Microsoft Ads and paid search have higher ROAS, suggesting that these channels are highly efficient in generating revenue relative to the cost.
TikTok Ads and FB & IG Ads have lower ROAS, suggesting lower revenue generation per euro spent on these channels, which may require further analysis and adjustment.

### 4. **CPO Across Channels**
[View Visualization](https://github.com/MrJohn91/Attribution-Pipeline-Orchestration/blob/main/images%3AScreenshots/CPO%20Across%20Channels.png)
- **Insight**: TikTok Ads and FB & IG Ads have the highest CPO, indicating they are more expensive to acquire a customer compared to other channels which have lower CPO values.

### 5. **IHC Phase Weights**
[View Visualization](https://github.com/MrJohn91/Attribution-Pipeline-Orchestration/blob/main/images%3AScreenshots/IHC%20phase%20weights.png)
- **Insight**: The IHC Phase Weights shows the importance of each phase in the customer journey. The Holder phase has the highest weight, indicating its greater influence on maintaining user engagement and pushing them closer to conversion, while the Initializer and Closer phases have similar, moderate weights.

## Conclusion

This pipeline automates the attribution of conversions to marketing channels using the **IHC Attribution Model**, powered by **Airflow** for automation and scheduling. It processes data from an **SQLite** database, sends it to the **IHC Attribution API**, and writes the results back to the database. Visualizations provide insights into channel performance, enabling more informed marketing decisions.

## Next Steps

1. **Cloud Integration**: Implement cloud-based solutions for storing and processing large volumes of data.
2. **Enhanced Error Handling**: Improve fault tolerance and error recovery mechanisms to ensure smooth operation.
3. **Real-Time Analytics**: Enable real-time processing of data to generate up-to-date attribution insights.
