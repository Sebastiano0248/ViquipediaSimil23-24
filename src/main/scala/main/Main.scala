package main

import java.io.File

// Per la primera part de la pràctica:
import scala.io.Source
import scala.collection.immutable.Set
import scala.math.sqrt

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import mapreduce._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

// Tenim dos objectes executables
// - tractaxml utilitza un "protoParser" per la viquipèdia i
// exampleMapreduce usa el MapReduce

object FuncionsPrimeraPartPractica {

  val PATH = System.getProperty("user.dir") + "/primeraPartPractica/"

  def main(fitxer: String, stopWordsFile: String, usarStopWords: Boolean): Unit = {
    mostrar(fitxer, stopWordsFile, usarStopWords)
  }

  // Funció mostrar, decideix si mostra freq o nonstopfreq
  def mostrar(fitxer: String, stopWordsFile: String, usarStopWords: Boolean): Unit = {
    val currentDir = System.getProperty("user.dir")
    println("El directorio actual es: " + currentDir)
    val text = Source.fromFile(PATH + fitxer).mkString
    val stopWords = if (usarStopWords) loadStopWords(stopWordsFile) else Set.empty[String]
    val freqMap = if (usarStopWords) nonstopfreq(text, stopWords) else freq(text)

    val totalWords = freqMap.values.sum
    val differentWords = freqMap.size
    println(s"Num de Paraules: $totalWords\tDiferents: $differentWords")

    println(f"Paraules\tocurrències\tfreqüència")
    println("-" * 50)
    freqMap.toList.sortBy(-_._2).take(10).foreach {
      case (word, count) => println(f"$word%-10s\t$count%-10d\t${(count.toDouble / totalWords) * 100}%.2f")
    }
  }

  // Funció per carregar les stop-words des d'un fitxer
  def loadStopWords(filePath: String): Set[String] = {
    Source.fromFile(PATH + filePath).getLines().map(_.trim.toLowerCase).toSet
  }

  // Funció per calcular la freqüència de paraules
  def freq(text: String): Map[String, Int] = {
    text.toLowerCase
      .replaceAll("'", " ")
      .replaceAll("[^a-zàáâãäåāăąçćĉċčďđèéêëēĕėęěĝğġģĥħìíîïīĭįıĵķĺļľŀłñńņňŋòóôõöøōŏőŕŗřśŝşšţťŧùúûüūŭůűųŵýÿŷźżž\\s]", "")
      .split("\\s+")
      .filter(_.nonEmpty)
      .groupBy(identity)
      .view.mapValues(_.length).toMap
  }

  // Funció per calcular la freqüència de paraules sense les stop-words
  def nonstopfreq(text: String, stopWords: Set[String]): Map[String, Int] = {
    text.toLowerCase
      .replaceAll("'", " ")
      .replaceAll("[^a-zàáâãäåāăąçćĉċčďđèéêëēĕėęěĝğġģĥħìíîïīĭįıĵķĺļľŀłñńņňŋòóôõöøōŏőŕŗřśŝşšţťŧùúûüūŭůűųŵýÿŷźżž\\s]", "")
      .split("\\s+")
      .filter(word => word.nonEmpty && !stopWords.contains(word))
      .groupBy(identity)
      .view.mapValues(_.length).toMap
  }

  def paraulafreqfreq(file: String): Unit = {
    val text = Source.fromFile(PATH + file).mkString
    val wordFreqs = freq(text)
    val freqOfFreqs = wordFreqs.values
      .groupBy(identity)
      .view.mapValues(_.size)
      .toMap

    val sortedFreqs = freqOfFreqs.toList.sortBy(-_._2)

    println("Les 10 freqüències més freqüents:")
    sortedFreqs.take(10).foreach {
      case (freq, count) => println(s"$count paraules apareixen $freq vegades")
    }

    println("\nLes 5 freqüències menys freqüents:")
    sortedFreqs.reverse.take(5).foreach {
      case (freq, count) => println(s"$count paraules apareixen $freq vegades")
    }
  }

  // Funció per generar els ngrams i comptar les seves freqüències
  def ngramFreq(fitxer: String, n: Int): Map[String, Int] = {
    val text = Source.fromFile(PATH + fitxer).mkString
    text.toLowerCase
      .replaceAll("'", " ")
      .replaceAll("[^a-zàáâãäåāăąçćĉċčďđèéêëēĕėęěĝğġģĥħìíîïīĭįıĵķĺļľŀłñńņňŋòóôõöøōŏőŕŗřśŝşšţťŧùúûüūŭůűųŵýÿŷźżž\\s]", "")
      .split("\\s+")
      .filter(_.nonEmpty)
      .sliding(n)
      .map(_.mkString(" "))
      .toList
      .groupBy(identity)
      .view.mapValues(_.length).toMap
  }

  def showNgramFreq(fitxer: String, n: Int): Unit = {
    val ngrams = ngramFreq(fitxer, n)

    println(s"\nLes 10 ngrams més freqüents de longitud $n:")
    ngrams.toList.sortBy(-_._2).take(10).foreach {
      case (ngram, count) => println(f"$ngram%-20s\t$count%-10d")
    }
  }

  def vector(fitxer1: String, fitxer2: String, stopWordsFile: String, n: Int): Unit = {
    val stopWords = loadStopWords(stopWordsFile)
    val text1 = Source.fromFile(PATH + fitxer1).mkString
    val text2 = Source.fromFile(PATH + fitxer2).mkString
    val similarity = cosinesim(text1, text2, stopWords, n)
    println(f"Cosine Similarity: $similarity%.4f")
  }

  // Funció per normalitzar les freqüències de paraules
  def normalizedFreq(wordFreq: Map[String, Int]): Map[String, Double] = {
    val maxFreq = wordFreq.values.max.toDouble
    wordFreq.view.mapValues(freq => freq / maxFreq).toMap
  }

  // Funció per calcular la similitud de cosinus entre dos documents
  def cosinesim(text1: String, text2: String, stopWords: Set[String], n: Int): Double = {
    if(n == 0){
      val freq1 = normalizedFreq(nonstopfreq(text1, stopWords))
      val freq2 = normalizedFreq(nonstopfreq(text2, stopWords))

      val allWords = freq1.keySet.union(freq2.keySet)
      val vec1 = allWords.toList.map(word => freq1.getOrElse(word, 0.0))
      val vec2 = allWords.toList.map(word => freq2.getOrElse(word, 0.0))

      val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum

      val magnitude1 = sqrt(vec1.map(a => a * a).sum)
      val magnitude2 = sqrt(vec2.map(b => b * b).sum)

      if (magnitude1 == 0 || magnitude2 == 0) 0.0 else dotProduct / (magnitude1 * magnitude2)
    } else{
      val freq1 = ngramFreq(text1, n)
      val freq2 = ngramFreq(text2, n)

      val allWords = freq1.keySet.union(freq2.keySet)
      val vec1 = allWords.toList.map(word => freq1.getOrElse(word, n).toDouble)
      val vec2 = allWords.toList.map(word => freq2.getOrElse(word, n).toDouble)

      val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum

      val magnitude1 = sqrt(vec1.map(a => a * a).sum)
      val magnitude2 = sqrt(vec2.map(b => b * b).sum)

      if (magnitude1 == 0 || magnitude2 == 0) 0.0 else dotProduct / (magnitude1 * magnitude2)
    }
  }
}


object fitxers extends App{
  // ProcessListStrings.mostrarTextDirectori("primeraPartPractica")
  FuncionsPrimeraPartPractica.main("pg11-net.txt", "", usarStopWords = false)
  FuncionsPrimeraPartPractica.main("pg11-net.txt", "english-stop.txt", usarStopWords = true)
  FuncionsPrimeraPartPractica.paraulafreqfreq("pg11-net.txt")
  FuncionsPrimeraPartPractica.showNgramFreq("pg11-net.txt", 3)
  FuncionsPrimeraPartPractica.vector("pg11.txt", "pg12.txt", "english-stop.txt", 0)
  FuncionsPrimeraPartPractica.vector("pg11.txt", "pg12.txt", "english-stop.txt", 1)
  FuncionsPrimeraPartPractica.vector("pg11.txt", "pg12.txt", "english-stop.txt", 2)
}

object tractaxml extends App {

  val parseResult= ViquipediaParse.parseViquipediaFile()

  parseResult match {
    case ViquipediaParse.ResultViquipediaParsing(t,c,r) =>
      println("TITOL: "+ t)
      println("CONTINGUT: ")
      println(c)
      println("REFERENCIES: ")
      println(r)
  }
}

object exampleMapreduce extends App {

  val nmappers = 1
  val nreducers = 1
  val f1 = new java.io.File("f1")
  val f2 = new java.io.File("f2")
  val f3 = new java.io.File("f3")
  val f4 = new java.io.File("f4")
  val f5 = new java.io.File("f5")
  val f6 = new java.io.File("f6")
  val f7 = new java.io.File("f7")
  val f8 = new java.io.File("f8")

  val fitxers: List[(File, List[String])] = List(
    (f1, List("hola", "adeu", "per", "palotes", "hola","hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f2, List("hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f3, List("que", "tal", "anem", "be")),
    (f4, List("be", "tal", "pericos", "pal")),
    (f5, List("doncs", "si", "doncs", "quin", "pal", "doncs")),
    (f6, List("quin", "hola", "vols", "dir")),
    (f7, List("hola", "no", "pas", "adeu")),
    (f8, List("ahh", "molt", "be", "adeu")))


  val compres: List[(String,List[(String,Double, String)])] = List(
    ("bonpeu",List(("pep", 10.5, "1/09/20"), ("pep", 13.5, "2/09/20"), ("joan", 30.3, "2/09/20"), ("marti", 1.5, "2/09/20"), ("pep", 10.5, "3/09/20"))),
    ("sordi", List(("pep", 13.5, "4/09/20"), ("joan", 30.3, "3/09/20"), ("marti", 1.5, "1/09/20"), ("pep", 7.1, "5/09/20"), ("pep", 11.9, "6/09/20"))),
    ("canbravo", List(("joan", 40.4, "5/09/20"), ("marti", 100.5, "5/09/20"), ("pep", 10.5, "7/09/20"), ("pep", 13.5, "8/09/20"), ("joan", 30.3, "7/09/20"), ("marti", 1.5, "6/09/20"))),
    ("maldi", List(("pepa", 10.5, "3/09/20"), ("pepa", 13.5, "4/09/20"), ("joan", 30.3, "8/09/20"), ("marti", 0.5, "8/09/20"), ("pep", 72.1, "9/09/20"), ("mateu", 9.9, "4/09/20"), ("mateu", 40.4, "5/09/20"), ("mateu", 100.5, "6/09/20")))
  )

  // Creem el sistema d'actors
  val systema: ActorSystem = ActorSystem("sistema")

  // funcions per poder fer un word count
  def mappingWC(file:File, words:List[String]) :List[(String, Int)] =
        for (word <- words) yield (word, 1)


  def reducingWC(word:String, nums:List[Int]):(String,Int) =
        (word, nums.sum)


  println("Creem l'actor MapReduce per fer el wordCount")
  val wordcount = systema.actorOf(Props(new MapReduce(fitxers,mappingWC,reducingWC )), name = "mastercount")

  // Els Futures necessiten que se'ls passi un temps d'espera, un pel future i un per esperar la resposta.
  // La idea és esperar un temps limitat per tal que el codi no es quedés penjat ja que si us fixeu preguntar
  // i esperar denota sincronització. En el nostre cas, al saber que el codi no pot avançar fins que tinguem
  // el resultat del MapReduce, posem un temps llarg (100000s) al preguntar i una Duration.Inf a l'esperar la resposta.

  // Enviem un missatge com a pregunta (? enlloc de !) per tal que inicii l'execució del MapReduce del wordcount.
  //var futureresutltwordcount = wordcount.ask(mapreduce.MapReduceCompute())(100000 seconds)

  implicit val timeout = Timeout(10000 seconds) // L'implicit permet fixar el timeout per a la pregunta que enviem al wordcount. És obligagori.
  var futureresutltwordcount = wordcount ? mapreduce.MapReduceCompute()

  println("Awaiting")
  // En acabar el MapReduce ens envia un missatge amb el resultat
  val wordCountResult:Map[String,Int] = Await.result(futureresutltwordcount,Duration.Inf).asInstanceOf[Map[String,Int]]

  println("Results Obtained")
  for(v<-wordCountResult) println(v)

  // Fem el shutdown del actor system
  println("shutdown")
  systema.terminate()
  println("ended shutdown")
  // com tancar el sistema d'actors.

  /*
  EXERCICIS:

  Useu el MapReduce per saber quant ha gastat cada persona.

  Useu el MapReduce per saber qui ha fet la compra més cara a cada supermercat

  Useu el MapReduce per saber quant s'ha gastat cada dia a cada supermercat.
   */


  println("tot enviat, esperant... a veure si triga en PACO")
}



