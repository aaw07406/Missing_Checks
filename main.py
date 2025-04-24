from functions import *


# ----- GLOBAL VARIABLES ----- #
import_df = pd.DataFrame()
HISTORY_FILE = "file_history.txt"
MAX_HISTORY = 10
merged_df = None  # Will hold the merged result for exporting


# ----- FILE HISTORY ----- #
# region

def normalize_string(s):
    """Remove special characters, extra spaces, and make lowercase."""
    return re.sub(r'\W+', '', str(s).strip().lower())


def smart_read_file(file_path, expected_keywords=["payment", "serial", "number"], max_scan_rows=15):
    is_excel = file_path.lower().endswith((".xls", ".xlsx"))
    read_fn = pd.read_excel if is_excel else pd.read_csv

    # Read the first few rows with no header
    preview_df = read_fn(file_path, header=None, nrows=max_scan_rows)

    # Limit to first 11 columns (A to K = 0 to 10)
    preview_df = preview_df.iloc[:, :11]

    header_row_index = None
    for i, row in preview_df.iterrows():
        normalized_cells = [normalize_string(cell) for cell in row.values]

        for cell in normalized_cells:
            if all(keyword in cell for keyword in expected_keywords):
                header_row_index = i
                break

        if header_row_index is not None:
            break

    if header_row_index is None:
        raise ValueError("Could not find a header row containing expected keywords like 'payment', 'serial', 'number'.")

    # Re-read using detected header row
    df = read_fn(file_path, header=header_row_index)
    return df


def on_textbox_double_click(event):
    line_index = results_textbox.index("@%s,%s linestart" % (event.x, event.y))
    line_text = results_textbox.get(line_index, f"{line_index} lineend").strip()

    # Extract file path after the number and dot (e.g., "1. C:\path\to\file.csv")
    if ". " in line_text:
        _, path = line_text.split(". ", 1)
        load_file(path)


def display_history():
    history = load_file_history()
    results_textbox.delete(1.0, "end")
    results_textbox.insert("end", "Previously used files:\n\n")
    for i, file in enumerate(history, start=1):
        results_textbox.insert("end", f"{i}. {file}\n")


def load_file_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            return [line.strip() for line in f.readlines()]
    return []


def save_file_history(history):
    with open(HISTORY_FILE, "w") as f:
        for entry in history[:MAX_HISTORY]:
            f.write(entry + "\n")


def save_file_to_history(file_path):
    try:
        with open(HISTORY_FILE, 'a') as f:
            f.write(file_path + "\n")
    except Exception as e:
        print("Error writing to history file:", e)


def display_file_history():
    try:
        if not os.path.exists(HISTORY_FILE):
            return
        with open(HISTORY_FILE, 'r') as f:
            file_list = f.read().splitlines()

        results_textbox.delete(1.0, "end")
        results_textbox.insert("end", "Recent Files:\n\n")
        for path in file_list[-5:][::-1]:  # Show last 5 files
            results_textbox.insert("end", f"{path}\n")

    except Exception as e:
        print("Error reading history file:", e)


def on_dropdown_select(choice):
    global import_df

    if not choice:
        return

    try:
        import_df = smart_read_file(choice)

        def truncate_cell(x, maxlen=30):
            return str(x)[:maxlen] + "..." if len(str(x)) > maxlen else x

        preview_df = import_df.iloc[:, :10].head(10).map(truncate_cell)
        preview = tabulate(preview_df, headers='keys', tablefmt='pretty', showindex=False)

        results_textbox.delete(1.0, "end")
        results_textbox.insert("end", preview)

        messagebox.showinfo("File Loaded", f"File '{os.path.basename(choice)}' loaded successfully!")

    except Exception as e:
        messagebox.showerror("File Load Error", f"Failed to load file:\n{e}")


# ----- IMPORT EXCEL/CSV ----- #
def load_file(file_path=None):
    global import_df

    if not file_path:
        file_path = fd.askopenfilename(filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")])
        if not file_path:
            return

    file_path = file_path.strip().strip('"').strip("'")
    print("Selected file path:", file_path)

    try:
        import_df = smart_read_file(file_path)

        # Clean column names
        import_df.columns = import_df.columns.str.replace(' ', '', regex=False)
        print(import_df.head())

        # Pretty preview like dropdown
        def truncate_cell(x, maxlen=30):
            return str(x)[:maxlen] + "..." if len(str(x)) > maxlen else x

        preview_df = import_df.iloc[:, :10].head(10).map(truncate_cell)
        preview = tabulate(preview_df, headers='keys', tablefmt='pretty', showindex=False)

        results_textbox.delete(1.0, "end")
        results_textbox.insert("end", preview)

        messagebox.showinfo("File Import", "File successfully imported!")

        # Save to history
        history = load_file_history()
        if file_path not in history:
            history.insert(0, file_path)
            save_file_history(history)

        # display_history()

    except Exception as e:
        messagebox.showerror("Error", f"Failed to load file:\n{str(e)}")

# endregion


# ----- PROCESSING ----- #
def process_query():
    # Get the current date and time
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    print(f"Current date: {current_date}")
    print(f"Current time: {current_time}")

    print("Starting the script...")
    print("Current working directory:", os.getcwd())

    results_textbox.delete(1.0, "end")
    results_textbox.insert("end", "Running query... please wait.")
    results_textbox.update_idletasks()  # Ensures UI updates immediately



    # ----- EXCEL IMPORT ----- #
    # Import the Excel file
    excel_df = import_df

    print("Excel file imported successfully.")
    print("Excel DataFrame shape:", excel_df.shape)

    # ----- SQL QUERY ----- #
    # Main query to check records in CHECK_WORKFLOW table

    main_query = fr'''
        SELECT a.*
        FROM AR.CHECK_WORKFLOW a
        WHERE 1 = 1
        AND a.DELETE_IND = 'N'
        AND a.RECORD_TYPE_RF = 'MCHK'
        --AND CONTRACT_NUM = '301'
        WITH UR;
    '''


    def query_checker(query) -> None:
        """Run main query and check if it returns any records."""
        df = get_db2_sql(query, arrprod)
        print(df.head())

        """Check if the query returns any records."""
        if df.empty:
            print("No records found.")
        else:
            print(df.shape[0], "records found!")
        
        return df


    # ----- LOGIC ----- #
    # Check if the query returns any records
    # Merge with the excel_df and compare the CHECKNUM column with the SQL_CHECKNUM column
    # Save the result to a new DataFrame where a match exists
    # Export them to a CSV file

    print('Running the query...')
    sql_df = query_checker(main_query)

    # Compare sql_df to excel_df
    try:
        print("Removing leading zeros...")

        sql_df['CHECK_NUM'] = sql_df['CHECK_NUM'].astype(str).str.lstrip('0')

        # Normalize Excel column names
        normalized_cols = {col.replace(" ", "").upper(): col for col in excel_df.columns}
        print("Normalized column names:", normalized_cols)

        key = 'PAYMENT/SERIALNUMBER'
        if key not in normalized_cols:
            raise ValueError("Expected 'Payment/Serial Number' column not found.")

        # Get the actual column name and rename it for merging
        original_col_name = normalized_cols[key]
        print(f"Detected column: {original_col_name}")

        # Rename column to a standard name
        excel_df.rename(columns={original_col_name: 'PAYMENT/SERIALNUMBER'}, inplace=True)
        excel_df['PAYMENT/SERIALNUMBER'] = excel_df['PAYMENT/SERIALNUMBER'].astype(str).str.lstrip('0')

        print('Leading zeros removed! Merging dataframes...')
        print(sql_df.columns, excel_df.columns)

        # Now we can safely merge
        print("Merging DataFrames...")

        global merged_df
        merged_df = pd.merge(sql_df, excel_df, left_on='CHECK_NUM', right_on='PAYMENT/SERIALNUMBER', how='inner')

        print("DataFrame after merge:")
        print(merged_df.head())

        if not merged_df.empty:
            messagebox.showinfo(
            "Records Found!",
            f"Missing checks found! Number of missing checks found: {merged_df.shape[0]}. Proceed with exporting."
        )

            
        elif merged_df.empty:
            messagebox.showerror("No Records Found", "No missing checks found.")
    
    except: 
        print("No records found in the merge.")
        messagebox.showerror("No Records Found", "No missing checks found.")
        merged_df = pd.DataFrame()


# ----- DISPLAY ON CONSOLE ----- #
# region
    results_df = merged_df.copy()
    print('Query successfully completed!')

    results_df = results_df[['RECORD_TYPE_RF', 'CARRIER_CD', 'CARRIER_NM', 'CHECK_NUM', 'CHECK_AMT', 'Process Date']]

    # Clear previous results
    results_textbox.delete(1.0, "end")

    try:
        # Assuming merged_df is the result of a query or function
        if not merged_df.empty:
            messagebox.showinfo("Query Completed!", 'Success! View results in application.')

            # Format the DataFrame into a nicely aligned table
            formatted_results = tabulate(results_df, headers='keys', tablefmt='pretty', showindex=False)

            monospace_font = CTkFont(family="Courier", size=12)
            results_textbox.configure(font=monospace_font)

            # Insert formatted results into the textbox
            results_textbox.insert("end", formatted_results)

        elif merged_df.empty:
            messagebox.showerror("Error", f"An error occurred: No results returned.\nPlease retry your inputs.")

        else:
            # Insert results into textbox
            results_textbox.insert("end", merged_df.to_string(index=False))

    except Exception as e:
        # Catch any exceptions and show an error message box
        messagebox.showerror("Unexpected Error", f"An error occurred: {str(e)}")
# endregion


# ----- EXPORT TO CSV/XLSX ----- #
def export_file():
    global merged_df
    if merged_df is None or merged_df.empty:
        messagebox.showwarning("No Data", "There is no data to export.")
        return

    file_type = file_format_var.get()
    file_ext = "xlsx" if file_type.upper() == "XLSX" else "csv"
    file_path = fd.asksaveasfilename(
        defaultextension=f".{file_ext}",
        filetypes=[(f"{file_ext.upper()} files", f"*.{file_ext}")],
        title="Save Exported File"
    )

    if not file_path:
        return

    try:
        if file_ext == "csv":
            merged_df.to_csv(file_path, index=False)
        elif file_ext == "xlsx":
            merged_df.to_excel(file_path, index=False, engine="openpyxl")

        messagebox.showinfo("Export Successful", f"File exported successfully as {file_ext.upper()}.")

    except Exception as e:
        messagebox.showerror("Export Error", f"Failed to export file.\n{e}")



# ----- Create the main window ----- #
# region
# Setup GUI
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("green")
root = ctk.CTk()
root.title("Missing Check Application")
root.geometry("710x590")

# Top Frame with Logo and Background
header_frame = ctk.CTkFrame(root, height=130, corner_radius=0)
header_frame.pack(fill="x")

# ----- LOAD IMAGES ----- #
current_dir = os.path.dirname(os.path.abspath(__file__))

app = root
app.iconbitmap(os.path.join(current_dir, "images", "search_icon.ico"))
# Use PNG image (recommended for taskbar + title bar)
icon = PhotoImage(file=os.path.join(current_dir, "images", "search_icon.png"))
app.iconphoto(True, icon)

image_dir = os.path.join(current_dir, "images", "app_bg.jpg")
logo_dir = os.path.join(current_dir, "images", "logo_no_bg.png")

bg_image = ctk.CTkImage(dark_image=Image.open(image_dir), size=(800, 130))
logo_image = ctk.CTkImage(dark_image=Image.open(logo_dir), size=(175, 60))

# Background image label (fills the whole header)
bg_image_label = ctk.CTkLabel(header_frame, image=bg_image, text="")
bg_image_label.place(x=0, y=0, relwidth=1, relheight=1)  # Stretch to fit

# Logo image label (centered on top of background)
logo_image_label = ctk.CTkLabel(header_frame, image=logo_image, text="")
logo_image_label.place(relx=0.5, rely=0.5, anchor="center")  # Center logo

# Load icons
export_icon = CTkImage(Image.open("images/export_icon.png"), size=(20, 20))
start_icon = CTkImage(Image.open("images/start_icon.png"), size=(20, 20))
clear_icon = CTkImage(Image.open("images/clear_icon.png"), size=(20, 20))
exit_icon = CTkImage(Image.open("images/exit_icon.png"), size=(20, 20))
browse_icon = CTkImage(Image.open("images/browse_icon.png"), size=(20, 20))

# File selection area
file_frame = ctk.CTkFrame(root)
file_frame.pack(pady=10)

file_label = ctk.CTkLabel(file_frame, text="Select a File:")
file_label.grid(row=0, column=0, padx=5)

file_dropdown = ctk.CTkComboBox(file_frame, values=load_file_history(), command=on_dropdown_select)
file_dropdown.grid(row=0, column=1, padx=5)

browse_button = ctk.CTkButton(file_frame, text="Browse", image=browse_icon, compound='left', command=load_file, font=("Segoe UI", 14, "bold"),
                              corner_radius=5,
                            border_width=1.5,
                            fg_color="#1c8c53",           # Deep green
                            hover_color="#29a06d",        # Lighter on hover
                            border_color="#ffffff",       # White border
                            text_color="white")
browse_button.grid(row=0, column=2, padx=5)

# Format Selection
format_frame = ctk.CTkFrame(root)
# format_frame.pack()

export_label = ctk.CTkLabel(format_frame, text="Export Format:")
export_label.grid(row=0, column=0, padx=5)
export_button = ctk.CTkButton(format_frame, text="Export", image=export_icon, compound="left", command=export_file, font=("Segoe UI", 14, "bold"),
                              corner_radius=5,
                            border_width=1.5,
                            fg_color="#1c8c53",           # Deep green
                            hover_color="#29a06d",        # Lighter on hover
                            border_color="#ffffff",       # White border
                            text_color="white")
export_button.grid(row=0, column=3, padx=5)


file_format_var = ctk.StringVar(value="CSV")
csv_radio = ctk.CTkRadioButton(format_frame, text="CSV", variable=file_format_var, value="CSV")
xlsx_radio = ctk.CTkRadioButton(format_frame, text="XLSX", variable=file_format_var, value="XLSX")
csv_radio.grid(row=0, column=1, padx=5)
xlsx_radio.grid(row=0, column=2, padx=5)

# Results textbox with scrollbars
textbox_frame = ctk.CTkFrame(root)
textbox_frame.pack(pady=10, padx=10, fill="both", expand=False)


# CTkTextbox with scrollbar support
results_textbox = ctk.CTkTextbox(textbox_frame, wrap="none", width=760, height=270)
results_textbox.configure(font=CTkFont(family="Courier", size=12))
results_textbox.pack(side="left", fill="both", expand=True)
results_textbox.bind("<Double-1>", on_textbox_double_click)


# Action Buttons
button_frame = ctk.CTkFrame(root)
button_frame.pack(pady=10)

run_button = ctk.CTkButton(button_frame, text="Start Process", image=start_icon, compound='left', command=process_query, font=("Segoe UI", 14, "bold"),
                           corner_radius=5,
                            border_width=1.5,
                            fg_color="#1c8c53",           # Deep green
                            hover_color="#29a06d",        # Lighter on hover
                            border_color="#ffffff",       # White border
                            text_color="white")

run_button.grid(row=0, column=0, padx=10)

clear_button = ctk.CTkButton(button_frame, text="Clear Results", image=clear_icon, compound='left', command=lambda: results_textbox.delete("1.0", "end"), font=("Segoe UI", 14, "bold"),
                             corner_radius=5,
                            border_width=1.5,
                            fg_color="#1c8c53",           # Deep green
                            hover_color="#29a06d",        # Lighter on hover
                            border_color="#ffffff",       # White border
                            text_color="white")
clear_button.grid(row=0, column=1, padx=10)

exit_button = ctk.CTkButton(button_frame, text="Exit", image=exit_icon, compound='left', command=root.destroy, font=("Segoe UI", 14, "bold"),
                            corner_radius=5,
                            border_width=1.5,
                            fg_color="#1c8c53",           # Deep green
                            hover_color="#29a06d",        # Lighter on hover
                            border_color="#ffffff",       # White border
                            text_color="white")
exit_button.grid(row=0, column=2, padx=10)

format_frame.pack()

# Status label
status_label = ctk.CTkLabel(root, text="Ready", text_color="gray")
status_label.pack(pady=5)

# Start loop
root.mainloop()
# endregion