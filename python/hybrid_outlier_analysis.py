"""
Hybrid Matched-Pair and Outlier Citation Analysis
==================================================
This script performs the definitive analysis for the research project by
combining two powerful methodologies:

1.  **Matched-Pair Comparison:** It first compares the average citation behaviors
    (cohesion vs. malice metrics) between the pre-matched case and control groups.
2.  **Outlier Detection:** It then uses an Isolation Forest model to identify
    anomalous authors based on their citation patterns, irrespective of their tier.

The script then bridges these two analyses to test the ultimate hypothesis:
Are bottom-tier (case) authors significantly more likely to be classified as
anomalous outliers than their top-tier (control) counterparts?
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import wilcoxon
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import IsolationForest
import os
import warnings

# --- Configuration ---
DB_PATH = "rolap.db"
# Contamination is the expected % of outliers. A value of 1-5% is typical.
OUTLIER_CONTAMINATION = 0.02 # i.e., we expect the top 2% to be anomalous
TIER_COLORS = {"Case": "#d62728", "Control": "#1f77b4"}
BASE_OUTPUT_DIR = "analysis_results"
warnings.filterwarnings('ignore', category=UserWarning)
sns.set_theme(style="whitegrid", palette="muted")


def calculate_paired_cles(case_data: pd.Series, control_data: pd.Series) -> float:
    """Calculates the Paired Common Language Effect Size."""
    if len(case_data) != len(control_data):
        raise ValueError("Series must have the same length for a paired CLES.")
    return np.mean(case_data.values > control_data.values)


def detect_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses Isolation Forest to detect anomalous authors based on their metrics.
    """
    print("\nDetecting anomalous authors using Isolation Forest...")
    
    # Define the features that characterize behavior
    features = [
        'coauthor_citation_rate',
        'avg_asymmetry',
        'max_asymmetry',
        'avg_velocity',
        'max_burst',
        'self_citation_rate'
    ]
    
    # Use RobustScaler to handle outliers in the feature distributions themselves
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(df[features])
    
    # Initialize and fit the model
    iso_forest = IsolationForest(
        contamination=OUTLIER_CONTAMINATION,
        random_state=42,
        n_estimators=100
    )
    # The model returns -1 for outliers and 1 for inliers
    df['is_outlier'] = iso_forest.fit_predict(X_scaled)
    # Convert to a more intuitive 1 for outliers, 0 for inliers
    df['is_outlier'] = df['is_outlier'].apply(lambda x: 1 if x == -1 else 0)
    
    print(f"Done. Identified {df['is_outlier'].sum()} outliers ({df['is_outlier'].mean():.2%}).")
    return df


def perform_final_analysis(pairs_df: pd.DataFrame, features_df: pd.DataFrame):
    """
    Performs the final statistical tests and generates visualizations.
    """
    # --- Step 1: Merge the outlier results back into the matched pairs ---
    case_features = features_df.rename(columns=lambda x: f'case_{x}' if x != 'orcid' else x).rename(columns={'orcid': 'case_orcid'})
    control_features = features_df.rename(columns=lambda x: f'control_{x}' if x != 'orcid' else x).rename(columns={'orcid': 'control_orcid'})
    
    final_df = pd.merge(pairs_df, case_features, on='case_orcid', how='left')
    final_df = pd.merge(final_df, control_features, on='control_orcid', how='left')
    final_df.fillna(0, inplace=True)

    # --- Step 2: Perform statistical tests ---
    print("\n" + "="*80)
    print("      FINAL HYPOTHESIS TESTING: Are Case authors more likely to be Outliers?")
    print("="*80)
    
    case_outliers = final_df['case_is_outlier']
    control_outliers = final_df['control_is_outlier']
    
    # The Paired CLES on the binary outlier flag is the most important result.
    cles = calculate_paired_cles(case_outliers, control_outliers)
    # We use Wilcoxon as a robust test, though a paired chi-square could also be used.
    stat, p_value = wilcoxon(case_outliers, control_outliers, alternative='greater', zero_method='zsplit')

    print(f"  - Primary Metric:          Probability of being an Outlier")
    print(f"  - Wilcoxon P-Value:        {p_value:<20.3e}")
    print(f"  - Paired CLES:             {cles:<25.2%}")
    if p_value < 0.05 and cles > 0.5:
        print("  - Conclusion:              HYPOTHESIS SUPPORTED. Case (Bottom-Tier) authors are")
        print("                           significantly more likely to be classified as anomalous outliers.")
    else:
        print("  - Conclusion:              HYPOTHESIS NOT SUPPORTED.")
        
    # --- Step 3: Profile the outliers ---
    outlier_authors_df = features_df[features_df['is_outlier'] == 1]
    print("\n--- Profile of Detected Outliers ---")
    print(outlier_authors_df.describe())

    # --- Step 4: Generate visualizations ---
    fig, axes = plt.subplots(1, 2, figsize=(20, 8))
    fig.suptitle("Final Analysis: Outlier Status in Matched Pairs", fontsize=20, weight='bold')
    
    # Panel A: Bar chart showing outlier prevalence by tier
    outlier_summary = final_df[['case_is_outlier', 'control_is_outlier']].mean().reset_index()
    outlier_summary.columns = ['Tier', 'Outlier Rate']
    outlier_summary['Tier'] = outlier_summary['Tier'].apply(lambda x: 'Case' if 'case' in x else 'Control')
    
    sns.barplot(data=outlier_summary, x='Tier', y='Outlier Rate', ax=axes[0],
                palette=TIER_COLORS, hue='Tier', legend=False)
    axes[0].set_title("A: Prevalence of Anomalous Authors by Tier", fontsize=16)
    axes[0].set_ylabel("Proportion Classified as Outlier")
    axes[0].set_xlabel("")
    axes[0].yaxis.set_major_formatter(plt.FuncFormatter('{:.1%}'.format))

    # Panel B: Scatter plot showing the trade-off between cohesion and malice
    features_df['Overall Anomaly Score'] = -iso_forest.score_samples(scaler.transform(features_df[features])) # Get raw anomaly score
    sns.scatterplot(data=features_df, x='coauthor_citation_rate', y='max_asymmetry',
                    hue='tier_type', style='is_outlier', size='Overall Anomaly Score',
                    sizes=(50, 500), alpha=0.6, ax=axes[1], palette=TIER_COLORS)
    axes[1].set_title("B: Behavioral Profile of Authors", fontsize=16)
    axes[1].set_xlabel("Cohesion Metric (Co-Author Citation Rate)")
    axes[1].set_ylabel("Malice Metric (Max. Asymmetry)")
    axes[1].legend(title="Legend")
    
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(f"{BASE_OUTPUT_DIR}/figure_final_hybrid_analysis.png", dpi=300)
    plt.close()
    print(f"\nFinal analysis figure saved to '{BASE_OUTPUT_DIR}/figure_final_hybrid_analysis.png'")


def main():
    """Main execution workflow."""
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    try:
        conn = sqlite3.connect(DB_PATH)
        # --- Load the two essential tables ---
        print("Loading final features and matched pairs data...")
        features_df = pd.read_sql_query("SELECT * FROM author_features_final", conn)
        matched_pairs_df = pd.read_sql_query("SELECT * FROM matched_pair_comparison", conn)
        
        # --- Run the outlier detection model ---
        # We need to define scaler and iso_forest in the main scope to be accessible later
        global scaler, iso_forest, features
        features = ['coauthor_citation_rate', 'avg_asymmetry', 'max_asymmetry', 'avg_velocity', 'max_burst', 'self_citation_rate']
        scaler = RobustScaler()
        X_scaled = scaler.fit_transform(features_df[features])
        iso_forest = IsolationForest(contamination=OUTLIER_CONTAMINATION, random_state=42, n_estimators=100)
        features_df['is_outlier'] = iso_forest.fit_predict(X_scaled)
        features_df['is_outlier'] = features_df['is_outlier'].apply(lambda x: 1 if x == -1 else 0)
        
        # --- Perform the final, combined analysis ---
        perform_final_analysis(matched_pairs_df, features_df)

    except (sqlite3.Error, pd.io.sql.DatabaseError, ValueError) as e:
        print(f"\n[ERROR] An error occurred: {e}")
        print(f"Please ensure the database '{DB_PATH}' exists and the complete SQL pipeline has been run successfully.")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("\nDatabase connection closed. Analysis complete.")


if __name__ == "__main__":
    main()