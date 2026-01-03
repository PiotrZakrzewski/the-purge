# Data on NYE vandalism

The purpose of this project is to gather openly available data on vandalism during new years eve. Motivated by 2025/2026 new years eve in the Netherlands.

## Data Source & Format

The data is harvested from [p2000-online.net](https://www.p2000-online.net/p2000.py). The website presents emergency services alerts (P2000 system) in a tabular HTML format.

### HTML Structure

The data is contained within a `<table>`. Each alert consists of a primary row with the main details, optionally followed by secondary rows containing specific unit information (capcodes).

**Primary Row Structure:**
```html
<tr>
    <td class="DT">DD-MM-YYYY HH:MM:SS</td>
    <td class="[ServiceClass]">Service Name</td>
    <td class="Regio">Region Name</td>
    <td class="Md">Message Content</td>
</tr>
```

**Key Columns:**
1.  **Timestamp (`class="DT"`)**: The date and time of the alert.
    *   **Format**: `DD-MM-YYYY HH:MM:SS` (e.g., `03-01-2026 10:34:46`).
2.  **Service (`class="Br" | "Am" | "Po"`)**: The emergency service involved.
    *   `Br`: Brandweer (Fire Brigade)
    *   `Am`: Ambulance
    *   `Po`: Politie (Police)
3.  **Region (`class="Regio"`)**: The safety region (e.g., `Hollands Midden`, `Haaglanden`).
4.  **Message (`class="Md"`)**: The main text of the alert, often containing codes, urgency levels (A1, P1), and location details.

**Secondary Row (Unit Details):**
These rows follow the primary row and typically list specific units or monitor codes alerted.
```html
<tr>
    <td></td>
    <td></td>
    <td></td>
    <td class="Oms[ServiceClass]">Unit Description / Capcode</td>
</tr>
```
*   The first three cells are empty.
*   The fourth cell has a class like `OmsBr`, `OmsAm`, or `OmsPo` and contains details like "Monitorcode" or specific vehicle numbers.
