import java.io.ByteArrayInputStream

import scala.collection.JavaConverters._
import org.openscience.cdk.AtomContainer
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.io.MDLV2000Reader
import de.zbit.jcmapper.fingerprinters.{EncodingFingerprint, FingerPrinterFactory}
import de.zbit.jcmapper.fingerprinters.FingerPrinterFactory.FingerprintType
import de.zbit.jcmapper.fingerprinters.features.IFeature

import scala.collection.mutable
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.{Success, Try}

val pathFPNames = Seq("ECFP", "ECFC", "DFS")
val corrFPNames = Seq("SHED", "CATS2D")

val fpNamesJCompoundMapper = Seq("ECFP", "ECFC", "SHED", "DFS", "CATS2D")

object FingerPrinter {
  lazy val pathFPS = pathFPNames.map { fpName: String => initializeFingerprinter(fpName) }
  lazy val corrFPS = corrFPNames.map { fpName: String => initializeFingerprinter(fpName) }
  lazy val reader = new MDLV2000Reader()

  def initializeFingerprinter(fpName: String): EncodingFingerprint = {
    val fingerprintType: FingerprintType = FingerprintType.valueOf(fpName)
    val fingerprint: EncodingFingerprint = FingerPrinterFactory.getFingerprinter(fingerprintType)
    fingerprint
  }

  def computeStringFingerprint(fp: EncodingFingerprint, molecule: IAtomContainer): Seq[String] = {
    val features: mutable.Buffer[IFeature] = fp.getFingerprint(molecule).asScala
    val featureStrings: Seq[String] = features.map { feature: IFeature => feature.featureToString(true) }
    featureStrings
  }

  def computeCombinedFingerprint(fp: EncodingFingerprint, molecule: IAtomContainer): (Seq[String], Seq[Double]) = {
    val features: mutable.Buffer[IFeature] = fp.getFingerprint(molecule).asScala
    val combinedFeatures: Seq[(String, Double)] = features.map { feature: IFeature => (feature.featureToString(true), feature.getValue()) }.filter {
      case (_, num) => num != 0.0
    }
    combinedFeatures.unzip
  }

  def parseAtomContainer(mol_file: String): IAtomContainer = {
    reader.setReader(new ByteArrayInputStream(mol_file.getBytes))
    val molecule: IAtomContainer = reader.read(new AtomContainer())
    molecule
  }
}

val schema = new StructType(
  Array(
    StructField("ECFP", ArrayType(StringType, true), true),
    StructField("ECFC", ArrayType(StringType, true), true),
    StructField("DFS", ArrayType(StringType, true), true),
    StructField("SHED_index", ArrayType(StringType, true), true),
    StructField("CATS2D_index", ArrayType(StringType, true), true),
    StructField("SHED_value", ArrayType(DoubleType, true), true),
    StructField("CATS2D_value", ArrayType(DoubleType, true), true)
  )
)

val testUDF = udf((mf: String) => {
  def computeFeatures(mol_file: String): Try[Row] = Try {
    val molecule = FingerPrinter.parseAtomContainer(mol_file)
    val stringFeatures: mutable.ListBuffer[Seq[String]] = FingerPrinter.pathFPS.map { fp: EncodingFingerprint => FingerPrinter.computeStringFingerprint(fp, molecule) }.to[mutable.ListBuffer]
    val combinedFeatures: Seq[(Seq[String], Seq[Double])] = FingerPrinter.corrFPS.map { fp: EncodingFingerprint => FingerPrinter.computeCombinedFingerprint(fp, molecule) }

    val numericFeatures: mutable.ListBuffer[Seq[Double]] = mutable.ListBuffer.empty

    for ((strFeatures, numFeatures) <- combinedFeatures) {
      stringFeatures += strFeatures
      numericFeatures += numFeatures
    }

    val stringRow: Row = Row(stringFeatures: _*)
    val numericRow: Row = Row(numericFeatures: _*)
    val row: Row = Row.merge(stringRow, numericRow)
    row
  }

  computeFeatures(mf) match {
    case Success(row) => row
    case _ => Row(null, null, null, null, null, null, null)
  }
}, schema)

// subset 10% for testing
val df = spark.read.parquet("/local00/bioinf/tpp/chembl_25/chembl_25_assays_flattened.parquet").repartition(200)
val df_filtered = df.filter(size(col("mol_file")).equalTo(1))

val df_cleaned = df_filtered.withColumn("mol_file_single", explode(col("mol_file"))).select(
  col("inchikey"), col("mol_file_single").alias("mol_file"), col("activity"), col("index")
)

val df_test = df_cleaned.withColumn("descriptors", testUDF(col("mol_file"))).select("inchikey", "mol_file", "activity", "index", "descriptors.*")
df_test.write.parquet("/local00/bioinf/tpp/chembl_25/chembl_25_semisparse.parquet")
