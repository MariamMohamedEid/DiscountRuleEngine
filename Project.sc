//object Main extends App {

  import java.time.LocalDate
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter
  import java.time.OffsetDateTime
  import java.time.temporal.ChronoUnit
  import scala.io.Source
  import java.io.File
  import java.io.PrintWriter

  import java.sql.DriverManager
  import java.sql.PreparedStatement
  import java.sql.Connection

// case class to hold the parsed records
  case class Transaction(
                          timestamp: LocalDateTime,
                          product: String,
                          expiryDate: LocalDate,
                          quantity: Int,
                          unitPrice: BigDecimal,
                          salesChannel: String,
                          paymentMethod: String
                        )

  val formatterDate = DateTimeFormatter.ofPattern("yyyy-MM-dd")
 // read input file into a line (excluding header)
  val source = Source.fromFile("D:/Scala/ScalaCode/src/main/resources/TRX1000.csv")
  //return an iterator over lines in file and convert it to a list , skips the header
  val lines = source.getLines().toList.tail
  // gives a collection of all parsed records ready to apply parsed
  // rules on them
  val transactions = lines.map { line =>
    val cols = line.split(",").map(_.trim)
    val timestamp = OffsetDateTime.parse(cols(0)).toLocalDateTime
    Transaction(
      timestamp = timestamp,
      product = cols(1).toLowerCase,
      expiryDate = LocalDate.parse(cols(2), formatterDate),
      quantity = cols(3).toInt,
      unitPrice = BigDecimal(cols(4)),
      salesChannel = cols(5).toLowerCase,
      paymentMethod = cols(6).toLowerCase
    )
  }

  val isExpiringSoon: Transaction => Boolean = trx =>
    ChronoUnit.DAYS.between(trx.timestamp.toLocalDate, trx.expiryDate) < 30 &&
      ChronoUnit.DAYS.between(trx.timestamp.toLocalDate, trx.expiryDate) >= 1

  val expiryDiscount: Transaction => BigDecimal = trx =>
    BigDecimal(30 - ChronoUnit.DAYS.between(trx.timestamp.toLocalDate, trx.expiryDate))


  val isCheeseOrWine: Transaction => Boolean = trx =>
    trx.product.startsWith("cheese") || trx.product.startsWith("wine")

  val cheeseOrWineDiscount: Transaction => BigDecimal = trx => trx.product match {
    case p if p.startsWith("cheese") => BigDecimal(10)
    case p if p.startsWith("wine") => BigDecimal(5)
  }


  val isMarch23: Transaction => Boolean = trx => {
    val date = trx.timestamp.toLocalDate
    date.getMonthValue == 3 && date.getDayOfMonth == 23
  }

  val march23Discount: Transaction => BigDecimal = _ => BigDecimal(50)


  val isQuantityEligible: Transaction => Boolean = trx => trx.quantity >= 6
  val quantityDiscount: Transaction => BigDecimal = trx => trx.quantity match {
    case q if q >= 6 && q <= 9 => BigDecimal(5)
    case q if q >= 10 && q <= 14 => BigDecimal(7)
    case q if q >= 15 => BigDecimal(10)
    case _ => BigDecimal(0) // Should never hit because of qualifier
  }

val isAppSale: Transaction => Boolean = trx => trx.salesChannel == "App"
val appSaleDiscount: Transaction => BigDecimal = trx => {
  val roundedQty = ((trx.quantity + 4) / 5) * 5
  (roundedQty / 5) * 5 // Each step of 5 gives +5%
}

val isVisaPayment: Transaction => Boolean = trx => trx.paymentMethod == "Visa"
val visaDiscount: Transaction => BigDecimal = _ => BigDecimal(5)


val discountRules: List[(Transaction => Boolean, Transaction => BigDecimal)] = List(
    (isExpiringSoon, expiryDiscount),
    (isCheeseOrWine, cheeseOrWineDiscount),
    (isMarch23, march23Discount),
    (isQuantityEligible, quantityDiscount),
    (isAppSale, appSaleDiscount),
    (isVisaPayment, visaDiscount)
  )

// for a given trx go through all rules
  def calculateDiscount(
                         trx: Transaction,
                         rules: List[(Transaction => Boolean, Transaction => BigDecimal)]
                       ): BigDecimal = {
    val matchedDiscounts = rules
      .filter { case (qualifier, _) => qualifier(trx) } // apply qualifying check
      .map { case (_, calculator) => calculator(trx) } // get discount amounts
      .sortBy(_.toDouble)(Ordering[Double].reverse) // sort descending
      .take(2)
  // avg top 1 or 2 discounts else 0
    if (matchedDiscounts.nonEmpty)
      matchedDiscounts.sum / matchedDiscounts.size
    else
      BigDecimal(0)
  }

  case class FinalTransaction(
                               transaction: Transaction, // the original row
                               discount: BigDecimal, // the calculated discount
                               finalPrice: BigDecimal // price after applying the discount
                             )

  val logWriter = new PrintWriter(new File("D:/Scala/ScalaCode/src/main/scala/Labs/rules_engine.log"))


  def log(level: String, message: String): Unit = {
    val timestamp = java.time.LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    logWriter.println(s"$timestamp     $level     $message")
    logWriter.flush()
  }

  def processTransactions(transactions: List[Transaction]): List[FinalTransaction] =
    transactions.map { trx =>
      val discount = calculateDiscount(trx, discountRules) // apply rules
      val finalPrice = trx.unitPrice * trx.quantity * (1 - discount / 100) // compute price
      log("INFO", s"Processed transaction for ${trx.product} with $discount% discount")
      FinalTransaction(trx, discount, finalPrice) // wrap into result case class
    }

  val finalResults = processTransactions(transactions)

  val url = "jdbc:postgresql://localhost:5432/retail_db"
  val user = "user"
  val password = "123"
  val conn: Connection = DriverManager.getConnection(url, user, password)
  val insertStmt: PreparedStatement = conn.prepareStatement(
    "INSERT INTO final_transactions (product, quantity, unit_price, discount, final_price) VALUES (?, ?, ?, ?, ?)"
  )

finalResults.foreach { ft =>
  val t = ft.transaction
  insertStmt.setString(1, t.product)
  insertStmt.setInt(2, t.quantity)
  insertStmt.setBigDecimal(3, t.unitPrice.bigDecimal)
  insertStmt.setBigDecimal(4, ft.discount.bigDecimal)
  insertStmt.setBigDecimal(5, ft.finalPrice.bigDecimal)
  insertStmt.addBatch()
}

insertStmt.executeBatch()
insertStmt.close()
conn.close()
logWriter.close()
source.close()
