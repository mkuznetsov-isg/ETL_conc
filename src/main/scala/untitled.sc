"".nonEmpty

List(List(), List(3), List(1,2)).flatten

"1605593222.887706".toDouble

val res = "1, 2,     3,4".split(",\\s*")
res.length

val args = """source=source1 tid=200 file="1, 2,  3,4" """

val file: String = """file\s*=\s*"([^"]+)"""".r.findFirstMatchIn(args) match {
  case Some(g) => g.group(1)
  case None => ""
}

val file2 = if (file.nonEmpty) file.split(",\\s*").toList else file