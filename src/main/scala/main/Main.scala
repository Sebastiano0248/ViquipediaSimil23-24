/*
  Fet per Genís Alvarado & Joan Massallé
*/

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

  val PATH = System.getProperty("user.dir") + "/primeraPartPractica/" // Camí a les dades

  // Funció mostrar freqüències, decideix si mostra les freqüències de paraules normals o sense stop-words
  def mostrarFreq(fitxer: String, stopWordsFile: String, usarStopWords: Boolean): Unit = {
    val currentDir = System.getProperty("user.dir")  // Obté el directori actual
    println("El directorio actual es: " + currentDir)

    // Llegeix el contingut del fitxer
    val text = Source.fromFile(PATH + fitxer).mkString

    // Carrega les stop-words si cal
    val stopWords = if (usarStopWords) loadStopWords(stopWordsFile) else Set.empty[String]
    // Calcula les freqüències de paraules, excloent les stop-words si cal
    val freqMap = if (usarStopWords) nonstopfreq(text, stopWords) else freq(text)

    // Calcula el nombre total de paraules i paraules diferents
    val totalWords = freqMap.values.sum
    val differentWords = freqMap.size
    println(s"Num de Paraules: $totalWords\tDiferents: $differentWords")

    // Mostra les 10 paraules més freqüents i la seva freqüència relativa
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

  def cleanText(text: String): Array[String] = {
    text.toLowerCase
      .replaceAll("'", " ")  // Substitueix les cometes per espais
      .replaceAll("[^\\p{L}\\s]", "")
      .split("\\s+")  // Divideix el text en paraules
      .filter(_.nonEmpty)  // Filtra les paraules buides
  }

  def freq(text: String): Map[String, Int] = {
    cleanText(text)  // Crida a cleanText que retorna un array de paraules
      .groupBy(identity)  // Agrupa les paraules pel seu valor (paraula)
      .view.mapValues(_.length).toMap  // Compta les ocurrències de cada paraula
  }

  def nonstopfreq(text: String, stopWords: Set[String]): Map[String, Int] = {
    cleanText(text)  // Crida a cleanText que retorna un array de paraules
      .filter(word => !stopWords.contains(word))  // Filtra les paraules de parada (stop-words)
      .groupBy(identity)  // Agrupa les paraules pel seu valor (paraula)
      .view.mapValues(_.length).toMap  // Compta les ocurrències de cada paraula
  }

  // Funció per mostrar les freqüències de les paraules més i menys freqüents
  def paraulafreqfreq(file: String): Unit = {
    val text = Source.fromFile(PATH + file).mkString
    val wordFreqs = freq(text)  // Calcula les freqüències de paraules
    val freqOfFreqs = wordFreqs.values
      .groupBy(identity)  // Agrupa les freqüències de les freqüències
      .view.mapValues(_.size)
      .toMap

    val sortedFreqs = freqOfFreqs.toList.sortBy(-_._2)  // Ordena per freqüència

    // Mostra les 10 freqüències més freqüents
    println("Les 10 freqüències més freqüents:")
    sortedFreqs.take(10).foreach {
      case (freq, count) => println(s"$count paraules apareixen $freq vegades")
    }

    // Mostra les 5 freqüències menys freqüents
    println("\nLes 5 freqüències menys freqüents:")
    sortedFreqs.reverse.take(5).foreach {
      case (freq, count) => println(s"$count paraules apareixen $freq vegades")
    }
  }

  // Funció per generar ngrams i comptar les seves freqüències
  def ngramFreq(fitxer: String, n: Int): Map[String, Int] = {
    val text = Source.fromFile(PATH + fitxer).mkString
    cleanText(text)  // Crida a cleanText per netejar i dividir el text
      .sliding(n)  // Genera ngrams de longitud n
      .map(_.mkString(" "))  // Uneix els ngrams en una cadena
      .toList
      .groupBy(identity)  // Agrupa els ngrams per identificar-los
      .view.mapValues(_.length).toMap  // Compta la freqüència de cada ngram
  }

  // Funció per mostrar els ngrams més freqüents
  def showNgramFreq(fitxer: String, n: Int): Unit = {
    val ngrams = ngramFreq(fitxer, n)  // Obté els ngrams i les seves freqüències

    // Mostra els 10 ngrams més freqüents
    println(s"\nLes 10 ngrams més freqüents de longitud $n:")
    ngrams.toList.sortBy(-_._2).take(10).foreach {
      case (ngram, count) => println(f"$ngram%-20s\t$count%-10d")
    }
  }

  // Funció per calcular la similitud de cosinus entre dos documents
  def vector(fitxer1: String, fitxer2: String, stopWordsFile: String, n: Int): Unit = {
    val stopWords = loadStopWords(stopWordsFile)  // Carrega les stop-words
    val similarity = cosinesim(fitxer1, fitxer2, stopWords, n)  // Calcula la similitud de cosinus
    if(n == 1) println(f"Cosine Similarity: $similarity%.4f")
    else if(n == 2) println(f"Cosine Similarity with digrams: $similarity%.4f")
    else if(n == 3) println(f"Cosine Similarity with trigrams: $similarity%.4f")
    else println(f"Cosine Similarity with ngrams: $similarity%.4f")
  }

  // Funció per normalitzar les freqüències de paraules
  def normalizedFreq(wordFreq: Map[String, Int]): Map[String, Double] = {
    val maxFreq = wordFreq.values.max.toDouble  // Obté la màxima freqüència entre totes les paraules
    // Normalitza les freqüències dividint cada freqüència per la màxima
    wordFreq.view.mapValues(freq => freq / maxFreq).toMap
  }

  // Funció per calcular la similitud de cosinus entre dos documents
  def cosinesim(fitxer1: String, fitxer2: String, stopWords: Set[String], n: Int): Double = {
    if(n == 1){  // Si n és 1, calcula la similitud de cosinus per paraules individuals
      val text1 = Source.fromFile(PATH + fitxer1).mkString  // Llegeix el primer document
      val text2 = Source.fromFile(PATH + fitxer2).mkString  // Llegeix el segon document

      // Calcula les freqüències normalitzades de les paraules dels dos textos sense les stop-words
      val freq1 = normalizedFreq(nonstopfreq(text1, stopWords))
      val freq2 = normalizedFreq(nonstopfreq(text2, stopWords))

      // Obté la unió de les paraules dels dos textos
      val allWords = freq1.keySet.union(freq2.keySet)

      // Construeix els vectors de les freqüències per cada paraula
      val vec1 = allWords.toList.map(word => freq1.getOrElse(word, 0.0))  // Vector de freqüències per al primer document
      val vec2 = allWords.toList.map(word => freq2.getOrElse(word, 0.0))  // Vector de freqüències per al segon document

      // Calcula el producte escalar dels dos vectors
      val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum

      // Calcula la magnitud (norma) de cada vector
      val magnitude1 = sqrt(vec1.map(a => a * a).sum)  // Magnitud del primer vector
      val magnitude2 = sqrt(vec2.map(b => b * b).sum)  // Magnitud del segon vector

      // Retorna la similitud de cosinus, assegurant-se que les magnituds no siguin zero
      if (magnitude1 == 0 || magnitude2 == 0) 0.0 else dotProduct / (magnitude1 * magnitude2)
    } else {  // Si n és més gran que 1, es calcula la similitud de cosinus per ngrams
      val freq1 = ngramFreq(fitxer1, n)  // Obtén les freqüències de ngrams pel primer document
      val freq2 = ngramFreq(fitxer2, n)  // Obtén les freqüències de ngrams pel segon document

      // Obté la unió de tots els ngrams dels dos documents
      val allWords = freq1.keySet.union(freq2.keySet)

      // Construeix els vectors de les freqüències per cada ngram
      val vec1 = allWords.toList.map(word => freq1.getOrElse(word, n).toDouble)  // Vector de freqüències per al primer document
      val vec2 = allWords.toList.map(word => freq2.getOrElse(word, n).toDouble)  // Vector de freqüències per al segon document

      // Calcula el producte escalar dels dos vectors
      val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum

      // Calcula la magnitud (norma) de cada vector
      val magnitude1 = sqrt(vec1.map(a => a * a).sum)  // Magnitud del primer vector
      val magnitude2 = sqrt(vec2.map(b => b * b).sum)  // Magnitud del segon vector

      // Retorna la similitud de cosinus, assegurant-se que les magnituds no siguin zero
      if (magnitude1 == 0 || magnitude2 == 0) 0.0 else dotProduct / (magnitude1 * magnitude2)
    }
  }
}

object fitxers extends App{

  FuncionsPrimeraPartPractica.showNgramFreq("pg11-net.txt", 1)
  FuncionsPrimeraPartPractica.showNgramFreq("pg11-net.txt", 2)
  FuncionsPrimeraPartPractica.showNgramFreq("pg12-net.txt", 3)
  FuncionsPrimeraPartPractica.showNgramFreq("pg74-net.txt", 5)

  FuncionsPrimeraPartPractica.vector("pg11-net.txt", "pg12-net.txt", "english-stop.txt", 1)
  FuncionsPrimeraPartPractica.vector("pg11-net.txt", "pg12-net.txt", "english-stop.txt", 2)
  FuncionsPrimeraPartPractica.vector("pg11-net.txt", "pg12-net.txt", "english-stop.txt", 3)

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

  // EXERCICI 1: Useu el MapReduce per saber quant ha gastat cada persona.

  val systema1: ActorSystem = ActorSystem("sistema")

  def mappingE1(lloc: String, compres: List[(String, Double, String)]) :List[(String, Double)] =
    for ((nom, preu, data) <- compres) yield (nom, preu)

  def reducingE1(nom:String, preu:List[Double]):(String,Double) =
    (nom, preu.sum)

  println("Creem l'actor MapReduce per calcular els consums") //  List[(File, List[String])]
  val gastatPersona = systema1.actorOf(Props(new MapReduce(compres,mappingE1,reducingE1 )), name = "mastercount")
  var futureresutltGastatPersona = gastatPersona ? mapreduce.MapReduceCompute()
  println("Esperant")
  val gastatPersonaResult:Map[String,Double] = Await.result(futureresutltGastatPersona,Duration.Inf).asInstanceOf[Map[String,Double]]

  println("Resultats obtinguts")
  for(v<-gastatPersonaResult) println(v)

  println("shutdown")
  systema1.terminate()
  println("ended shutdown")

  // EXERCICI 2: Useu el MapReduce per saber qui ha fet la compra més cara a cada supermercat

  val systema2: ActorSystem = ActorSystem("sistema")

  def mappingE2(lloc: String, compres: List[(String, Double, String)]) :List[(String, Double)] =
    for ((nom, preu, data) <- compres) yield (nom, preu)

  def reducingE2(nom:String, preu:List[Double]):(String,Double) =
    (nom, preu.max)

  println("Creem l'actor MapReduce per calcular els consums màxims")
  val maxGastatPersona = systema2.actorOf(Props(new MapReduce(compres,mappingE2,reducingE2 )), name = "mastercount")
  var futureresutltMaxGastatPersona = maxGastatPersona ? mapreduce.MapReduceCompute()
  println("Esperant")
  val maxGastatPersonaResult:Map[String,Double] = Await.result(futureresutltMaxGastatPersona,Duration.Inf).asInstanceOf[Map[String,Double]]

  println("Resultats obtinguts")
  for(v<-maxGastatPersonaResult) println(v)

  println("shutdown")
  systema2.terminate()
  println("ended shutdown")

  // EXERCICI 3: Useu el MapReduce per saber quant s'ha gastat cada dia a cada supermercat.

  val systema3: ActorSystem = ActorSystem("sistema")

  def mappingE3(lloc: String, compres: List[(String, Double, String)]) :List[((String,String), Double)] =
    for ((nom, preu, data) <- compres) yield ((lloc, data), preu)

  def reducingE3(tupla:(String, String), preu:List[Double]):((String,String),Double) =
    (tupla, preu.sum)

  println("Creem l'actor MapReduce per calcular els consums a cada supermercat cada dia")
  val gastatPerDia = systema3.actorOf(Props(new MapReduce(compres,mappingE3,reducingE3 )), name = "mastercount")
  var futureresutltGastatPerDia = gastatPerDia ? mapreduce.MapReduceCompute()
  println("Esperant")
  val gastatPerDiaResult:Map[String,Double] = Await.result(futureresutltGastatPerDia,Duration.Inf).asInstanceOf[Map[String,Double]]

  println("Resultats obtinguts")
  for(v<-gastatPerDiaResult) println(v)

  println("shutdown")
  systema3.terminate()
  println("ended shutdown")

  println("tot enviat, esperant... a veure si triga en PACO")
}


