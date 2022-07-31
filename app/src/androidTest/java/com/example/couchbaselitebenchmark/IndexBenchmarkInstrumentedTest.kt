package com.example.couchbaselitebenchmark

import android.util.Log
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.couchbase.lite.*
import org.junit.*
import org.junit.runner.RunWith
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.zip.ZipInputStream
import kotlin.time.Duration.Companion.nanoseconds


/**
 * Instrumented test, which will execute on an Android device.
 *
 * See [testing documentation](http://d.android.com/tools/testing).
 */
@RunWith(AndroidJUnit4::class)
class IndexBenchmarkInstrumentedTest {

    companion object {
        private lateinit var database1: Database
        private lateinit var database2: Database

        @BeforeClass @JvmStatic fun setup() {
            val context = InstrumentationRegistry.getInstrumentation().targetContext
            val assetManager = context.assets

            CouchbaseLite.init(context)
            unzip(assetManager.open("travel-sample.zip"), context.filesDir, "travel-sample")
            unzip(assetManager.open("travel-sample.zip"), context.filesDir, "travel-sample_index")

            val config = DatabaseConfiguration()
            config.directory = String.format("%s", context.filesDir)
            database1 = Database("travel-sample", config)
            database2 = Database("travel-sample_index", config)

            database2.createIndex("index_routeTypeSourceAirport",
                IndexBuilder.valueIndex(
                    ValueIndexItem.property("type"),
                    ValueIndexItem.property("sourceairport")
                ))

            Thread.sleep(10000);
        }

        @AfterClass @JvmStatic fun teardown() {
            database1.delete()
            database1.close()

            database2.delete()
            database2.close()
        }

        @Throws(IOException::class)
        private fun unzip(`in`: InputStream, destination: File, destinationFileName: String) {
            val buffer = ByteArray(1024)
            val zis = ZipInputStream(`in`)
            var ze = zis.nextEntry
            while (ze != null) {
                val tmpFileName = ze.name.substringAfter(".")
                val fileName = "$destinationFileName.$tmpFileName"
                //val fileName = ze.name

                val newFile = File(destination, fileName)
                if (ze.isDirectory) {
                    newFile.mkdirs()
                } else {
                    File(newFile.parent).mkdirs()
                    val fos = FileOutputStream(newFile)
                    var len: Int
                    while (zis.read(buffer).also { len = it } > 0) {
                        fos.write(buffer, 0, len)
                    }
                    fos.close()
                }
                ze = zis.nextEntry
            }
            zis.closeEntry()
            zis.close()
            `in`.close()
        }
    }

    @Test
    fun benchmarkWithoutIndex() {

        Log.d("Document count", database1.count.toString())
        Log.d("Index", database1.indexes.toString())

        val query = database1.let { DataSource.database(it) }.let {
            QueryBuilder.select(
                SelectResult.expression(Meta.id),
                SelectResult.all()
            )
                .from(it)
                .where(
                    Expression.property("type").equalTo(Expression.string("route"))
                        .and(
                            Expression.property("sourceairport").equalTo(Expression.string("LYS"))
                        )
                )
        }

        val result: ResultSet = query.execute()
        Log.d("Result count", result.count().toString())

        val countBenchmark = 1000
        val start= System.nanoTime()

        for (i in 1..countBenchmark) {
            query.execute()
        }

        val elapsed = ((System.nanoTime() - start) / countBenchmark).nanoseconds
        Log.d("Benchmark without Index", "Average time query execution time was $elapsed")

    }

    @Test
    fun benchmarkWithIndex() {

        Log.d("Document count", database2.count.toString())
        Log.d("Index", database2.indexes.toString())

        val query = database2.let { DataSource.database(it) }.let {
            QueryBuilder.select(
                SelectResult.expression(Meta.id),
                SelectResult.all()
            )
                .from(it)
                .where(
                    Expression.property("type").equalTo(Expression.string("route"))
                        .and(
                            Expression.property("sourceairport").equalTo(Expression.string("LYS"))
                        )
                )
        }

        val result: ResultSet = query.execute()
        Log.d("Result count", result.count().toString())

        val countBenchmark = 1000
        val start= System.nanoTime()

        for (i in 1..countBenchmark) {
            query.execute()
        }

        val elapsed = ((System.nanoTime() - start) / countBenchmark).nanoseconds
        Log.d("Benchmark with Index", "Average time query execution time was $elapsed")
    }
}