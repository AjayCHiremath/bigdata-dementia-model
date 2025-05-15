# 🧠 Real-Time Dementia Prediction using Big Data & PySpark

## 📌 Overview

This project builds a real-time big data processing pipeline to predict dementia phenotypes using PySpark, HDFS, and Apache Flume. It features live data ingestion, batch-based feature selection, real-time machine learning model training, and performance evaluation. The pipeline is optimized for streaming, parallel batch processing, and scalable ML workflows on structured health datasets.

---

## 🔄 Workflow

### 🔹 1. Data Loading & Ingestion
- Uses HDFS and Apache Flume for real-time data flow
- Processes raw and batch-ingested `.csv` files
- Reads data using Spark structured streaming

### 🔹 2. Data Preprocessing
- Null column filtering (drops columns with >10 nulls)
- Missing value imputation (with placeholder values)
- Type casting (all features to `IntegerType`)
- Columns vectorized via `VectorAssembler`

### 🔹 3. Feature Selection
- Applies `ChiSqSelector` to identify top 2 features per batch
- Appends selected features from all batches into a master list
- Adds a unique `id` column for merging target and features

### 🔹 4. Combining Batches
- Uses `monotonically_increasing_id()` to assign unique IDs
- Joins multiple DataFrames on `id`
- Combines into a final dataset with labels and selected features

### 🔹 5. Vectorization & Modeling
- Reads combined features and vectorizes using `VectorAssembler`
- Retains only `PHENOTYPE` and `features` columns for modeling

---

## 📈 Model Training & Evaluation

### Models Used:
- Logistic Regression (`LogisticRegression`)
- Random Forest Classifier (`RandomForestClassifier`)
- Decision Tree Classifier (`DecisionTreeClassifier`)

### Pipeline:
- Train-test split (80/20)
- Separate `Pipeline` objects per model
- Predictions made on test data
- Redundant columns removed for clarity

### Evaluation:
- Evaluated using `MulticlassClassificationEvaluator`
- Metric: **Accuracy**
- Comparison between models

---

## 📊 Results Summary

| Model                     | Accuracy       |
|--------------------------|----------------|
| **Logistic Regression**  | **98.78%** ✅   |
| Random Forest Classifier | 71.95%         |
| Decision Tree Classifier | 74.39%         |

> **Conclusion**: Logistic Regression significantly outperformed other models in accuracy.

---

## ⚙️ Technologies Used

- **Apache Spark** (Structured Streaming, MLlib)
- **Apache Flume** (real-time ingestion)
- **HDFS** (storage)
- **Python** with PySpark API
- **Chi-Square Selector** for feature filtering
- **VectorAssembler** for feature vectorization

---

## 🚀 How to Run

### Prerequisites
- Spark 3.x
- Hadoop & HDFS setup
- Apache Flume agent
- Python 3.8+ with PySpark
- Jupyter Notebook (for development/debugging)

### Steps

1. **Start HDFS and Flume Agent**
   - Ensure data streams to the HDFS ingestion path.
2. **Run Spark Streaming Script**
   - Executes feature extraction, vectorization, and DataFrame merging.
3. **Train Models**
   - Run model training code to fit Logistic Regression, Random Forest, and Decision Tree.
4. **Evaluate**
   - Review prediction accuracy and select the best model.
