import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import s3fs

# Minio Configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_KEY = "minio"
MINIO_SECRET = "minio123"
BUCKET_NAME = "nyc-processed"
FILE_NAME = "yellow_tripdata_2023-01_clean.parquet"


def get_minio_fs():
    """
    Creates and returns an S3FileSystem object connected to Minio.

    Returns
    -------
    s3fs.S3FileSystem
        Configured S3 file system objects.
    """
    return s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': MINIO_ENDPOINT},
        key=MINIO_KEY,
        secret=MINIO_SECRET,
        use_listings_cache=False
    )


def load_data(fs):
    """
    Load Parquet data from Minio.

    Parameters
    ----------
    fs : s3fs.S3FileSystem
        File system objects connected to Minio.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing taxi data.
    """
    file_path = f"{BUCKET_NAME}/{FILE_NAME}"
    print(f"Reading data from {file_path} ...")

    df = pd.read_parquet(file_path, filesystem=fs)
    return df


def preprocess_features(df):
    """
    Feature Engineering: Extract temporal features and select columns for training.

    Parameters
    ----------
    df : pd.DataFrame
         Original DataFrame.

    Returns
    -------
    pd.DataFrame, pd.Series
        The processed feature matrix X and target vector y.
    """
    print("Feature engineering in progress...")

    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek

    features = [
        'trip_distance',
        'RatecodeID',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'pickup_hour',
        'day_of_week'
    ]

    target = 'total_amount'

    X = df[features]
    y = df[target]

    return X, y


def build_pipeline():
    """
    Build a Scikit-Learn Pipeline that includes preprocessing and models.

    Returns
    -------
    sklearn.pipeline.Pipeline
        Configured training pipeline.
    """
    # Numeric Features (No special handling required, or can be standardized)
    numeric_features = ['trip_distance', 'passenger_count', 'pickup_hour', 'day_of_week']

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features),
        ],
        remainder='passthrough'
    )

    pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', LinearRegression())
    ])

    return pipeline


def train_and_evaluate(X, y):
    """
    Partition the dataset, train the model, and evaluate its performance.

    Parameters
    ----------
    X : pd.DataFrame
        Feature matrix
    y : pd.Series
        Target vector
    """
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"Begin training the model (train dataset size: {len(X_train)})...")
    model = build_pipeline()
    model.fit(X_train, y_train)

    print("Evaluating the model...")
    predictions = model.predict(X_test)

    rmse = np.sqrt(mean_squared_error(y_test, predictions))

    print("-" * 30)
    print("Model Evaluation Results:")
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")

    if rmse < 10:
        print("✅ Success! RMSE < 10")
    else:
        print("❌ Warning! RMSE > 10")
    print("-" * 30)

    # Print Prediction Sample Comparison:
    comparison = pd.DataFrame({'Actual': y_test, 'Predicted': predictions}).head(5)
    print("\nPrediction Sample Comparison::\n", comparison)


if __name__ == "__main__":
    fs = get_minio_fs()

    df = load_data(fs)

    X, y = preprocess_features(df)

    train_and_evaluate(X, y)
