# Week 5 Data Pipeline Maintenance

## Pipeline Ownership

### 1. **Profit Pipeline**
   - **Primary Owner**: Alice (Data Engineer)
   - **Secondary Owner**: Bob (Data Engineer)

### 2. **Unit-level Profit Pipeline (Experiments)**
   - **Primary Owner**: Charlie (Data Engineer)
   - **Secondary Owner**: Alice (Data Engineer)

### 3. **Aggregate Profit Pipeline (Investors)**
   - **Primary Owner**: Bob (Data Engineer)
   - **Secondary Owner**: Charlie (Data Engineer)

### 4. **Growth Pipeline**
   - **Primary Owner**: David (Data Engineer)
   - **Secondary Owner**: Alice (Data Engineer)

### 5. **Daily Growth Pipeline (Experiments)**
   - **Primary Owner**: Alice (Data Engineer)
   - **Secondary Owner**: David (Data Engineer)

### 6. **Aggregate Growth Pipeline (Investors)**
   - **Primary Owner**: Bob (Data Engineer)
   - **Secondary Owner**: David (Data Engineer)

### 7. **Engagement Pipeline**
   - **Primary Owner**: Charlie (Data Engineer)
   - **Secondary Owner**: Bob (Data Engineer)

### 8. **Aggregate Engagement Pipeline (Investors)**
   - **Primary Owner**: David (Data Engineer)
   - **Secondary Owner**: Charlie (Data Engineer)

---

## On-Call Schedule

To ensure fairness and account for holidays, the on-call schedule rotates weekly among the four team members. Holidays are evenly distributed across the team.

### Weekly Rotation:
- **Week 1**: Alice
- **Week 2**: Bob
- **Week 3**: Charlie
- **Week 4**: David

### Holiday Coverage:
- **New Year's Day**: David
- **Memorial Day**: Alice
- **Independence Day**: Bob
- **Labor Day**: Charlie
- **Thanksgiving**: David
- **Christmas**: Alice

---

## Run Books for Pipelines Reporting Metrics to Investors

### 1. **Aggregate Profit Pipeline**
   - **Purpose**: Calculate and report aggregate profit metrics to investors.
   - **Steps**:
     1. Extract raw profit data from the database.
     2. Aggregate profit by region, product, and time period.
     3. Validate data for accuracy and completeness.
     4. Load aggregated data into the reporting dashboard.
   - **Monitoring**: Check for data delays, missing values, and outliers.

### 2. **Aggregate Growth Pipeline**
   - **Purpose**: Calculate and report aggregate growth metrics to investors.
   - **Steps**:
     1. Extract raw growth data from the database.
     2. Aggregate growth by region, product, and time period.
     3. Validate data for accuracy and completeness.
     4. Load aggregated data into the reporting dashboard.
   - **Monitoring**: Check for data delays, missing values, and outliers.

### 3. **Aggregate Engagement Pipeline**
   - **Purpose**: Calculate and report aggregate engagement metrics to investors.
   - **Steps**:
     1. Extract raw engagement data from the database.
     2. Aggregate engagement by region, product, and time period.
     3. Validate data for accuracy and completeness.
     4. Load aggregated data into the reporting dashboard.
   - **Monitoring**: Check for data delays, missing values, and outliers.

---

## Potential Pipeline Issues

### 1. **Data Delays**
   - Raw data may not arrive on time due to upstream system failures or network issues.

### 2. **Data Quality Issues**
   - Missing or incorrect data fields could lead to inaccurate aggregations.

### 3. **Pipeline Failures**
   - Code errors, resource constraints, or infrastructure failures could cause pipeline crashes.

### 4. **Schema Changes**
   - Changes in the source data schema could break the pipeline if not handled properly.

### 5. **Monitoring Gaps**
   - Lack of proper monitoring could delay the detection of pipeline issues.

### 6. **Holiday Coverage**
   - Pipeline issues during holidays may take longer to resolve due to reduced staffing.

---

## Conclusion
This document outlines the ownership, on-call schedule, run books, and potential issues for the pipelines. It ensures accountability, fairness, and preparedness for maintaining the data pipelines effectively.