from book import Book, Node, from_list, get_logger, add_property
import csv
import numpy as np

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def detect_anomalies(data: list, z_score_threshold: float = 2.0) -> dict:
    """
    Detects anomalies in a list of numbers using the Z-score method.
    """
    revenues = np.array([float(row['revenue']) for row in data])
    mean = np.mean(revenues)
    std = np.std(revenues)
    
    anomalies = {}
    for i, revenue in enumerate(revenues):
        z_score = (revenue - mean) / std
        if abs(z_score) > z_score_threshold:
            anomalies[i] = {
                "value": revenue,
                "z_score": z_score,
                "mean": mean,
                "std": std
            }
    return anomalies

def main():
    """
    This script demonstrates AI-powered anomaly detection in financial data.
    """
    logger.info("Starting anomaly detection use case...")

    # 1. Load monthly sales data into a Book
    logger.info("Loading monthly sales data...")
    sales_data = load_csv("monthly_sales.csv")
    sales_nodes = [Node(name=row["month"], properties=row) for row in sales_data]
    sales_book = Book(name="Monthly Sales", root_nodes=sales_nodes)

    # 2. Perform anomaly detection (simulating Researcher)
    logger.info("Performing anomaly detection...")
    anomalies = detect_anomalies(sales_data)

    # 3. Flag anomalies in the Book
    logger.info("Flagging anomalies in the Book...")
    if anomalies:
        for index, anomaly_info in anomalies.items():
            node = sales_book.root_nodes[index]
            node.flags["is_anomaly"] = True
            add_property(node, "anomaly_details", anomaly_info)

    # 4. Print the anomaly report
    logger.info("\n--- Anomaly Detection Report ---")
    if not anomalies:
        logger.info("No anomalies detected.")
    else:
        for node in sales_book.root_nodes:
            if node.flags.get("is_anomaly"):
                details = node.properties.get("anomaly_details", {})
                print(f"Anomaly Detected in Month: {node.name}")
                print(f"  Revenue: ${details.get('value'):,.2f}")
                print(f"  Z-Score: {details.get('z_score'):.2f} (Threshold: 2.0)")
                print(f"  Mean: ${details.get('mean'):,.2f}, Std Dev: ${details.get('std'):,.2f}")
                print("-" * 40)

    logger.info("Anomaly detection use case completed.")

if __name__ == "__main__":
    main()
