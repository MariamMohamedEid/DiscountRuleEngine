# DiscountRuleEngine

This Scala project implements a functional rule engine that processes retail transaction data, applies a series of predefined business rules to determine applicable discounts, and calculates the final price for each transaction. The program reads transaction records from a CSV file, processes them using a pure functional approach, logs each processed transaction, and writes the final results to a databse.

## Project Objectives

- Read and parse transaction data from a CSV file.
- Apply a set of business rules to determine discount eligibility.
- Select the top two applicable discount rules for each transaction and average their values.
- Calculate the final price after discount.
- Log each processed transaction to a file.
- Export the final processed transactions to a databse

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

5. **App-Based Discount**  
   - Transactions made via the **App** channel receive:  
   - +5% per group of 5 units (rounded up)

6. **Visa Payment Discount**  
   - Payments made using **Visa** → 5% discount

If more than one rule qualifies, the system averages the top two discounts and uses that value in the final price calculation.

## File Structure

![image](https://github.com/user-attachments/assets/b1d97544-69a7-4621-a68a-2e94e3ea43bc)
## 🛠 How to Run

1. Open the project in IntelliJ IDEA or your Scala environment.
2. Make sure `TRX1000.csv` is located at the specified path in the code (`src/main/resources/`) or adjust the path.
3. Set up a PostgreSQL database and table:
   ```sql
   CREATE DATABASE retail_db;

   \c retail_db

   CREATE TABLE final_transactions (
     product TEXT,
     quantity INTEGER,
     unit_price DECIMAL,
     discount DECIMAL,
     final_price DECIMAL
   );
   Update DB credentials in the code:

## Update DB credentials in the code 
- val url = "jdbc:postgresql://localhost:5432/retail_db"
- val user = "user"
- val password = "123"

## Add dependencies in build.sbt:
- libraryDependencies += "org.typelevel" %% "cats-core" % "2.10.0"
- libraryDependencies += "org.postgresql" % "postgresql" % "42.7.3"
## Run the main application:
sbt run
