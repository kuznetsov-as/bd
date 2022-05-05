import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.postgresql.util.PSQLException
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.sql.Connection
import java.sql.DriverManager


fun main() {

    var string = "Время\tПолная дата\tH-/Конус (мА)\tH-/Линза Воблова/Слив (°C)\tH-/Линза Воблова/Напор (°C)\tH-/Линза Воблова/Мощность (Вт)\tH-/Линза Воблова/Поток (л/мин)\tH-/Напор IFM (л/мин)\tH-/Слив IFM (л/мин)\tH-/Источник (Па)\tH-/Дифф. откачка (Па)\tBergoz/HEBL/Ток (мА)\tКонус/1/Ток (мА)\tКонус/1/Центр (°C)\tКонус/1/М Верх (°C)\tКонус/1/М Право (°C)\tКонус/1/М Низ (°C)\tКонус/1/М Лево (°C)\tКонус/1/Б Верх (°C)\tКонус/1/Б Право (°C)\tКонус/1/Б Низ (°C)\tКонус/1/Б Лево (°C)\tКонус/2/Ток (мА)\tКонус/2/Центр (°C)\tКонус/2/М Верх (°C)\tКонус/2/М Право (°C)\tКонус/2/М Низ (°C)\tКонус/2/М Лево (°C)\tКонус/2/Б Верх (°C)\tКонус/2/Б Право (°C)\tКонус/2/Б Низ (°C)\tКонус/2/Б Лево (°C)\tЭЛВ/Энергия (кэВ)\tЭЛВ/Beam0 (мА)\tЭЛВ/EnergyU get (кВ)\tЭЛВ/EnergyU set (кВ)\tЭЛВ/EnergyI (мА)\tЭЛВ/Avarage (мА)\tЭЛВ/Sec1U (кВ)\tЭЛВ/Sec1I (мА)\tЭЛВ/IsolatorU (кВ)\tЭЛВ/IsolatorI (мА)\tЭЛВ/Dark (мА)\tОхл. диаф./1/Верх (°C)\tОхл. диаф./1/Право (°C)\tОхл. диаф./1/Низ (°C)\tОхл. диаф./1/Лево (°C)\tОхл. диаф./2/Верх (°C)\tОхл. диаф./2/Право (°C)\tОхл. диаф./2/Низ (°C)\tОхл. диаф./2/Лево (°C)\tОхл. диаф./3/Верх (°C)\tОхл. диаф./3/Право (°C)\tОхл. диаф./3/Низ (°C)\tОхл. диаф./3/Лево (°C)\tОхл. диаф./4/Верх (°C)\tОхл. диаф./4/Право (°C)\tОхл. диаф./4/Низ (°C)\tОхл. диаф./4/Лево (°C)\tОхл. диаф./5/Верх (°C)\tОхл. диаф./5/Право (°C)\tОхл. диаф./5/Низ (°C)\tОхл. диаф./5/Лево (°C)\tLi мишень/Ток (мА)\tLi мишень/Центр (0) (°C)\tLi мишень/Низ-право (1) (°C)\tLi мишень/Право (2) (°C)\tLi мишень/Верх-право (3) (°C)\tLi мишень/Верх (4) (°C)\tLi мишень/Верх-лево (5) (°C)\tLi мишень/Лево (6) (°C)\tLi мишень/Низ-лево (7) (°C)\tLi мишень/Вода вход (л/мин)\tLi мишень/Вода выход 1 (л/мин)\tLi мишень/Вода выход 2 (л/мин)\tLi мишень/Вакуум (Па)\tLi мишень 9/Центр (0) (°C)\tLi мишень 9/М Право (1) (°C)\tLi мишень 9/Б Право (2) (°C)\tLi мишень 9/М Верх (3) (°C)\tLi мишень 9/Б Верх (4) (°C)\tLi мишень 9/М Лево (5) (°C)\tLi мишень 9/Б Лево (6) (°C)\tLi мишень 9/М Низ (7) (°C)\tLi мишень 9/Б Низ (8) (°C)\tБоп-1М/Б2/Зал/Гамма (Зв/Ч)\tБоп-1М/Б2/Зал/Нейтроны (Зв/Ч)\tБоп-1М/Б2/Кор/Гамма (Зв/Ч)\tБоп-1М/Б2/Кор/Нейтроны (Зв/Ч)\tБоп-1М/Б3/Зал/Гамма (Зв/Ч)\tБоп-1М/Б3/Зал/Нейтроны (Зв/Ч)\tБоп-1М/Б3/Кор/Гамма (Зв/Ч)\tБоп-1М/Б3/Кор/Нейтроны (Зв/Ч)\tLi6/Нейтроны (шт/сек)\tLi6/Гамма (шт/сек)\tLi6/Загрузка (%)\tLi6/Интеграл нейтронов (шт)\tЭфф. обдирки/Нейтралы I (мА)\tЭфф. обдирки/Эфф. (%)\tUltravoltPs/300V/U set (В)\tUltravoltPs/300V/I set (мА)\tUltravoltPs/300V/U get (В)\tUltravoltPs/300V/I get (мА)\tБДН/A/n (шт/сек)\tБДН/A/g (шт/сек)\tБДН/A/gn (шт/сек)\tБДН/A/gSv (Зв/Ч)\tБДН/A/nSv (Зв/Ч)\tБДН/B/n (шт/сек)\tБДН/B/g (шт/сек)\tБДН/B/gn (шт/сек)\tБДН/B/gSv (Зв/Ч)\tБДН/B/nSv (Зв/Ч)\tБДН/C/n (шт/сек)\tБДН/C/g (шт/сек)\tБДН/C/gn (шт/сек)\tБДН/C/gSv (Зв/Ч)\tБДН/C/nSv (Зв/Ч)\tОбдир миш/Слив (°C)\tОбдир миш/Напор (°C)\tОбдир миш/Мощность (Вт)\tОбдир миш/Поток (л/мин)\tОбдир миш/Аргон\tHPGe/Мёртвое время (%)\tHPGe/Скорость в интеграле (шт/сек)\tHPGe/Интеграл (шт)\tВычислятор флюенса (Bergoz/HEBL/Ток)/Флюенс (мАч)\tВычислятор флюенса (ЭЛВ/Beam0)/Флюенс (мАч)\tВычислятор флюенса (Боп-1М/Б2/Зал/Гамма)/Флюенс (мАч)\tВычислятор флюенса (Боп-1М/Б2/Зал/Нейтроны)/Флюенс (мАч)\tВычислятор флюенса (Li6/Нейтроны)/Флюенс (мАч)\tУскоритель/Входная охлаждаемая диафрагма/Слив (°C)\tУскоритель/Входная охлаждаемая диафрагма/Напор (°C)\tУскоритель/Входная охлаждаемая диафрагма/Мощность (Вт)\tУскоритель/Входная охлаждаемая диафрагма/Поток (л/мин)\tВакуум/Ускоритель выход (Па)\tВакуум/HEBL После развертки (Па)\toperatorPcDT\tjournal"
    val headers: List<String> = string.split("\t")
    println(headers.lastIndex)

    val jdbcUrl = "jdbc:postgresql://localhost:5432/BNCT"
    val connection = DriverManager
        .getConnection(jdbcUrl, "postgres", "password")




    val pathName = "D:\\BNCT_DATA"

    File(pathName).walk().forEach {
        if (it.isFile) {
            try {
                saveInDB(readCSV(it.path), connection)
            } catch (e: PSQLException) {
                //println(e.localizedMessage)
            }

            //println(it.name)
        }
    }
}

fun saveInDB(bnctList: ArrayList<BNCT>, connection: Connection) {

    bnctList.forEach {
        val query = connection.prepareStatement("INSERT INTO mydata VALUES ('${it.date}','${it.cons_ma}','${it.linza}')")
        query.execute()
    }
}

fun checkHeaders(headers: String) {
    //println("В заголовках нет ничего лишнего")
}

fun readCSV(path: String): ArrayList<BNCT> {

    val result = arrayListOf<BNCT>()

    val reader = BufferedReader(InputStreamReader(FileInputStream(path), "Windows-1251"))

    val csvParser = CSVParser(reader, CSVFormat.DEFAULT
        .withFirstRecordAsHeader()
        .withDelimiter('	'))

    for (csvRecord in csvParser) {

        try {

            checkHeaders(csvRecord.get(0))

            val date = csvRecord.get("Полная дата")
            val cons_ma = csvRecord.get("H-/Конус (мА)")
            val linza = csvRecord.get("H-/Линза Воблова/Слив (°C)")

            result.add(BNCT(date, cons_ma, linza))

        } catch (e: IllegalArgumentException) {
            println("произошла беда: ${e.message}")
        }
    }

    return result

}


data class BNCT (
    val date: String,
    val cons_ma: String,
    val linza: String
)