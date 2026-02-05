# Use Case 11: AI-Powered Anomaly Detection in Financial Data (Researcher & Book)

This use case demonstrates how to use the `Book` library to structure time-series data and a script (simulating the `Researcher`) to perform anomaly detection on it. It highlights:

*   Using `Book` to represent a time-series dataset.
*   Applying a custom Python function for statistical analysis (anomaly detection).
*   Flagging and annotating data points with quality issues directly within the `Book` structure.

## Components Involved

*   **`Book` Library:** Used to structure the monthly sales data, where each month is a `Node`.
*   **`Researcher` (Simulated):** The `run_anomaly_detection.py` script acts as a "Researcher" that performs the analytical task of detecting anomalies.

## Files

*   `monthly_sales.csv`: A sample CSV file containing monthly revenue data with a few anomalies (unusually low and high values).
*   `run_anomaly_detection.py`: The Python script that loads the data, detects anomalies using the Z-score method, and generates a report.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Anomaly Detection Script

Navigate to the `Book/use_cases/11_anomaly_detection` directory and run the `run_anomaly_detection.py` script:

```bash
python run_anomaly_detection.py
```

### 3. What's Happening?

1.  **Load Data into Book:** The script loads the `monthly_sales.csv` file and creates a `Book` where each month is a `Node`.
2.  **Anomaly Detection (Researcher Simulation):** The `detect_anomalies` function calculates the mean and standard deviation of the revenue data. It then calculates the Z-score for each data point and identifies any points where the Z-score exceeds a predefined threshold (2.0 in this case).
3.  **Flagging Anomalies:** The script iterates through the identified anomalies and adds a `is_anomaly` flag and detailed information about the anomaly (the value, Z-score, mean, and standard deviation) to the properties of the corresponding `Node` in the `Book`.
4.  **Generate Report:** Finally, the script prints a report listing all detected anomalies and the statistical details that led to their identification.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting anomaly detection use case...
INFO:__main__:Loading monthly sales data...
INFO:__main__:Performing anomaly detection...
INFO:__main__:Flagging anomalies in the Book...

--- Anomaly Detection Report ---
Anomaly Detected in Month: 2025-08
  Revenue: $500,000.00
  Z-Score: 2.51 (Threshold: 2.0)
  Mean: $170,416.67, Std Dev: $131,223.32
----------------------------------------

INFO:__main__:Anomaly detection use case completed.
```

This use case demonstrates how the `Book` can serve as a powerful in-memory data structure for analytical workflows. By representing data as a `Book`, you can easily attach metadata (like anomaly flags and scores) directly to the data points, which can then be used for further analysis, visualization, or reporting.
