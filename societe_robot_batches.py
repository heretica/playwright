from playwright.sync_api import sync_playwright, TimeoutError
import pandas as pd
import re 

csv_file = 'dirty_data/liste_entreprises_siren.csv'
num_rows_to_read = 100000

df = pd.read_csv(csv_file, nrows=num_rows_to_read)
df['siren'] = df['siren'].astype(str).str.zfill(9)

liste_entreprises = df['siren'].unique().tolist()

# Set the starting batch number to resume from
starting_batch_number = 127

batch_size = 50  # Define batch size here
batch_number = starting_batch_number

def run(playwright, search_value):
    try:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto("https://www.societe.com/")
        page.locator("#input_search").click()
        page.locator("#input_search").fill(search_value)
        page.locator("#input_search").press("Enter")
        page.locator("#result_deno_societe div").first.click()
        page.locator("#synthese")
        text = page.locator("#synthese").inner_text()

        siret_num = page.locator("#siret_number").inner_text()
        code_naf = page.locator('#ape-histo-description').inner_text()

        arrow_tds = page.query_selector_all("td.arrow")
        adresse_postale_td = None
        for td in arrow_tds:
            if "Adresse postale" in td.inner_text():
                adresse_postale_td = td
                break

        adresse_text = "Adresse postale not found."
        if adresse_postale_td:
            sibling_td = page.evaluate_handle('(element) => element.nextElementSibling', adresse_postale_td)
            adresse_a = sibling_td.query_selector("a.Lien.secondaire")
            if adresse_a:
                adresse_text = adresse_a.inner_text()

        data = [{'siren': search_value, 'text': text, 'siret_num': siret_num, 'code_naf': code_naf, 'adresse_text': adresse_text}]
        
        

        return data
    except TimeoutError:
        print(f"Timeout error occurred for search value: {search_value}")
        return []
    finally:
        # Close the context and browser after scraping, regardless of exceptions
        context.close()
        browser.close()

with sync_playwright() as p:
    total_processed = (starting_batch_number - 1) * batch_size  # Calculate the total processed rows
    
    while total_processed < num_rows_to_read:
        current_batch = liste_entreprises[total_processed:total_processed + batch_size]
        final_data = []
        
        for idx, search_value in enumerate(current_batch, start=1):
            scraped_data = run(p, search_value)
            final_data.extend(scraped_data)
            print(f"Rows added in this run ({idx}/{batch_size}): {len(scraped_data)}")
            total_processed += len(scraped_data)
            print(f"Total rows processed: {total_processed}")
        
        scraped_df = pd.DataFrame(final_data)

        # Define a regular expression pattern to match names
        name_pattern = r"^([a-zA-Z]{2,}\s[a-zA-Z]{1,}'?-?[a-zA-Z]{2,}\s?([a-zA-Z]{1,})?)"  

        # Function to extract names from a given text using the pattern
        def extract_names(text):
            return re.findall(name_pattern, text)

        # Apply the function to your DataFrame column
        scraped_df['direction_noms'] = scraped_df['text'].apply(extract_names)

        # Display the DataFrame with extracted names
        print(scraped_df[['siren', 'direction_noms']])

        # Print the number of rows in the current DataFrame
        print(f"Rows in current DataFrame: {len(scraped_df)}")

        # Save the DataFrame to a CSV file
        csv_filename = f'scraped_data_batch_{batch_number}.csv'
        scraped_df.to_csv(csv_filename, index=False)
        
        print(f"Saved {csv_filename}")
        
        total_processed += batch_size
        batch_number += 1
        print(f"Processed {total_processed} values")
