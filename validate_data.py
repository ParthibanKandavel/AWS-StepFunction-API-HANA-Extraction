import pandas as pd

def validate_claims(filepath):
    df = pd.read_csv(filepath)
    if df.isnull().sum().any():
        print("❌ Data contains null values")
    else:
        print("✅ Data looks clean")

if __name__ == "__main__":
    validate_claims("sample_data/sample_claims.csv")
