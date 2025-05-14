# DiscountRuleEngine

This Scala project implements a functional rule engine that processes retail transaction data, applies a series of predefined business rules to determine applicable discounts, and calculates the final price for each transaction. The program reads transaction records from a CSV file, processes them using a pure functional approach, logs each processed transaction, and writes the final results to a separate CSV file.

## Project Objectives

- Read and parse transaction data from a CSV file.
- Apply a set of business rules to determine discount eligibility.
- Select the top two applicable discount rules for each transaction and average their values.
- Calculate the final price after discount.
- Log each processed transaction to a file.
- Export the final processed transactions to a new CSV file.

## Business Rules Implemented

1. **Expiry-Based Discount**  
   If the product expires in less than 30 days, apply a discount that increases as the expiry date gets closer.  
   For example:  
   - 29 days to expire → 1% discount  
   - 1 day to expire → 29% discount  

2. **Product Type Discount**  
   - If the product starts with "cheese", apply a 10% discount  
   - If it starts with "wine", apply a 5% discount  

3. **Special Day Discount**  
   - If the transaction occurs on March 23rd, apply a 50% discount  

4. **Quantity-Based Discount**  
   - Quantity between 6 and 9 → 5%  
   - Quantity between 10 and 14 → 7%  
   - Quantity 15 or more → 10%  

If more than one rule qualifies, the system averages the top two discounts and uses that value in the final price calculation.

## File Structure

![image](https://github.com/user-attachments/assets/b1d97544-69a7-4621-a68a-2e94e3ea43bc)
## How to Run

1. Open the project in IntelliJ IDEA or your preferred Scala environment.
2. Ensure the input file `TRX1000.csv` is located in `src/main/scala/Labs/` or just make sure to adjust your path accordingly.
3. Run the `DiscountRuleEngine.scala` file.
4. After the program runs:
   - A log file named `rules_engine.log` will be generated.
   - The final output will be saved in `final_transactions.csv`.
