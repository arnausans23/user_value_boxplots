import pandas as pd
import plotly.express as px

def run_flexible_cohort_analysis(data_frames_dict):
    """
    Flexible Master function that executes the full pipeline for ANY number of inputs.
    
    Args:
        data_frames_dict (dict): A dictionary where keys are the source names (e.g., 'Android', 'iOS')
                                 and values are the raw DataFrames.
                                 Example: {'Android': df_and, 'iOS': df_ios, 'Web': df_web}
    
    Returns: The final processed DataFrame containing all sources combined.
    """
    
    # --- STEP 1: DATA PREPARATION & MERGING ---
    
    processed_dfs = []

    # Iterate through the dictionary to process whatever number of files were passed
    for source_name, df_raw in data_frames_dict.items():
        # Create a copy to avoid SettingWithCopy warnings
        df_temp = df_raw.copy()
        
        # Tag the source (This replaces the hardcoded 'OS' assignment)
        # The key from the dictionary becomes the label in the plot
        df_temp['OS'] = source_name 
        
        processed_dfs.append(df_temp)

    # Concatenate all DataFrames found in the list
    if not processed_dfs:
        return "Error: No dataframes provided."
        
    df_combined = pd.concat(processed_dfs, ignore_index=True)

    # Sort by Date and Source to ensure chronological order
    df_combined = df_combined.sort_values(by=['Acquisition Week', 'OS'])

    # Logic to sync "Week Number" across all sources:
    unique_dates = sorted(df_combined['Acquisition Week'].unique())
    date_to_week_map = {date: f"Week {i+1}" for i, date in enumerate(unique_dates)}
    
    df_combined['Week Number'] = df_combined['Acquisition Week'].map(date_to_week_map)

    # Define the specific weeks of interest (from Week 8 to Week 40)
    weeks_of_interest = [
        'Week 8', 'Week 12', 'Week 16', 'Week 20', 
        'Week 24', 'Week 28', 'Week 32', 'Week 36', 'Week 40'
    ]

    # Define the columns we want to keep
    target_columns = [
        'Acquisition Week', 'Week Number', 'OS', 
        'Total Subscriptions', 'Total Unsubscriptions', 'Subs Remaining', 
        'Payout Total (€)', 'Total Gross (€)', 'Margin'
    ] + weeks_of_interest 
    
    # Filter to keep only existing columns (safety check)
    final_cols = [col for col in target_columns if col in df_combined.columns]
    df_final = df_combined[final_cols]


    # --- STEP 2: DISPERSION ANALYSIS (GROUPED BOXPLOT) ---
    
    # Check which of the requested weeks actually exist in the data
    cols_to_melt = [col for col in weeks_of_interest if col in df_final.columns]
    
    if cols_to_melt:
        # Melt dataframe to long format
        df_long = df_final.melt(
            id_vars=['OS'], 
            value_vars=cols_to_melt, 
            var_name='Cohort_Period', 
            value_name='Value (€)'
        )

        # Generate the Boxplot
        # Plotly handles 'color' dynamically, so it will create as many colors as keys in your dict
        fig_box = px.box(
            df_long, 
            x='Cohort_Period',    
            y='Value (€)',        
            color='OS',           # Uses the dictionary keys as legend names
            points="all",         
            title='Cohort Performance Distribution (Multi-Source Comparison)',
            category_orders={'Cohort_Period': weeks_of_interest} 
        )
        
        # Update layout to ensure boxes are grouped side-by-side
        fig_box.update_layout(boxmode='group') 
        fig_box.show()
    else:
        print("Warning: No columns found for the requested weeks (Week 8, 12, etc.) in the data.")

    return df_final
