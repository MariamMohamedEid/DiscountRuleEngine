object Main extends App {

  import java.time.LocalDate
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter
  import java.time.OffsetDateTime
  import java.time.temporal.ChronoUnit
  import scala.io.Source
  import java.io.File
  import java.io.PrintWriter

  case class Transaction(
                          timestamp: LocalDateTime,
                          product: String,
                          expiryDate: LocalDate,
                          quantity: Int,
                          unitPrice: BigDecimal
                        )

  val formatterDate = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val lines = Source.fromFile("D:/ITI/Scala/ScalaCode/src/main/resources/TRX1000.csv").getLines().toList.tail
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
      unitPrice = BigDecimal(cols(4))
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


  val discountRules: List[(Transaction => Boolean, Transaction => BigDecimal)] = List(
    (isExpiringSoon, expiryDiscount),
    (isCheeseOrWine, cheeseOrWineDiscount),
    (isMarch23, march23Discount),
    (isQuantityEligible, quantityDiscount)
  )

  def calculateDiscount(
                         trx: Transaction,
                         rules: List[(Transaction => Boolean, Transaction => BigDecimal)]
                       ): BigDecimal = {
    val matchedDiscounts = rules
      .filter { case (qualifier, _) => qualifier(trx) } // apply qualifying check
      .map { case (_, calculator) => calculator(trx) } // get discount amounts
      .sortBy(_.toDouble)(Ordering[Double].reverse) // sort descending
      .take(2)

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

  val logWriter = new PrintWriter(new File("D:/ITI/Scala/ScalaCode/src/main/scala/Labs/rules_engine.log"))

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

  val outputWriter = new PrintWriter("D:/ITI/Scala/ScalaCode/src/main/scala/Labs/final_transactions.csv")
  outputWriter.println("product,quantity,unit_price,discount,final_price")

  finalResults.foreach { ft =>
    val t = ft.transaction
    outputWriter.println(s"${t.product},${t.quantity},${t.unitPrice},${ft.discount},${ft.finalPrice}")
  }

  outputWriter.close()
  logWriter.close()

}