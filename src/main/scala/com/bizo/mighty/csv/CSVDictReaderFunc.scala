package com.bizo.mighty.csv

import au.com.bytecode.opencsv.{ CSVReader => OpenCSVReader }
import java.io.{ FileReader, InputStream, InputStreamReader, FileInputStream }
import scala.util.Try

/** Reads rows as Iterator[Map[String, String]]. The keys will be determined by 
 *  the header row (1st row). The header keys are then mapped to their respective data
 *  for each row. 
 *  
 *  @param reader the instance of OpenCSVReader to wrap
 */
case class CSVDictReaderFunc(reader: OpenCSVReader, errorIfColumnCountWrong: Boolean = true) extends Iterator[Try[Map[String, String]]] {

  private[this] val rows: Iterator[Row] = new CSVRowIterator(reader) flatten

  val header: Row = {
    if (!rows.hasNext) sys.error("No header found") else rows.next()
  }

  override def hasNext(): Boolean = {
    rows.hasNext
  }

  override def next(): Try[Map[String, String]] =
    Try {
      val currentRow = rows.next()
      if (header.length != currentRow.length && errorIfColumnCountWrong) {
        sys.error("Column mismatch: expected %d-cols. encountered %d-cols\nLine: [[[%s]]]".format(header.length,
          currentRow.length, currentRow.mkString("[-]")))
      } else Map(header.zip(currentRow): _*)
    }

  def close() {
    reader.close()
  }
}
