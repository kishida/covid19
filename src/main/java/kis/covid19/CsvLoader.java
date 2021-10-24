package kis.covid19;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CsvLoader {

    static final String baseUrl = "https://covid19.mhlw.go.jp/public/opendata/%s.csv";
    static final String confirmedCasesCumulativeDaily = String.format(baseUrl, "confirmed_cases_cumulative_daily");
    static final String requiringInpatientCareEtcDaily = String.format(baseUrl, "requiring_inpatient_care_etc_daily");
    static final String deathsCumulativeDaily = String.format(baseUrl, "deaths_cumulative_daily");
    static final String severeCasesDaily = String.format(baseUrl, "severe_cases_daily");
    static final String[] prefsRomaji = new String[]{
            "ALL",
            "Hokkaido",
            "Aomori", "Iwate", "Miyagi", "Akita", "Yamagata", "Fukushima",
            "Ibaraki", "Tochigi", "Gunma", "Saitama", "Chiba", "Tokyo", "Kanagawa",
            "Niigata", "Toyama", "Ishikawa", "Fukui", "Yamanashi", "Nagano",
            "Gifu", "Shizuoka", "Aichi", "Mie",
            "Shiga", "Kyoto", "Osaka", "Hyogo", "Nara", "Wakayama",
            "Tottori", "Shimane", "Okayama", "Hiroshima", "Yamaguchi",
            "Tokushima", "Kagawa", "Ehime", "Kochi",
            "Fukuoka", "Saga", "Nagasaki", "Kumamoto", "Oita", "Miyazaki", "Kagoshima", "Okinawa"
    };
    static final String[] prefsKanji = new String[]{
            "全国",
            "北海道",
            "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
            "岐阜県", "静岡県", "愛知県", "三重県",
            "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
            "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県",
            "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
    };

    /**
     * @param args パース可能な日付表現の配列。省略した場合は現在までの日付の配列が採用される。
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        var dates = createDates(args);

        for (var date : dates) {
            createPrefJson(date);
        }

        CreateData.main(args);
    }

    static List<LocalDate> createDates(final String[] args) {
        if (args.length > 0) {
            return Arrays.stream(args).map(LocalDate::parse).collect(Collectors.toList());
        } else {
            var files = new File("data").listFiles();
            if (files != null) {
                var existDateTexts = Arrays.stream(files)
                        .flatMap(file -> {
                            var regex = Pattern.compile("prefs(\\d{4}-\\d{2}-\\d{2}).json");
                            var match = regex.matcher(file.getName());
                            if (match.matches()) {
                                return Stream.of(match.group(1));
                            } else {
                                return Stream.empty();
                            }
                        })
                        .collect(Collectors.toList());
                var dates = new ArrayList<LocalDate>();
                var date = LocalDate.now();
                while (!existDateTexts.contains(date.format(DateTimeFormatter.ISO_LOCAL_DATE))) {
                    dates.add(date);
                    date = date.minusDays(1);
                }
                return dates;
            } else {
                return Collections.emptyList();
            }
        }
    }

    static void createPrefJson(final LocalDate date) throws IOException {
        var targetDate = date.minusDays(1);
        var patients = readCsv(targetDate, confirmedCasesCumulativeDaily, 0);
        var hospitalizations = readCsv(targetDate, requiringInpatientCareEtcDaily, 0);
        var discharges = readCsv(targetDate, requiringInpatientCareEtcDaily, 1);
        var mortality = readCsv(targetDate, deathsCumulativeDaily, 0);
        var severe = readCsv(targetDate, severeCasesDaily, 0);

        var prefs = IntStream.range(1, prefsRomaji.length)
                .mapToObj(prefIndex -> {
                    var prefRomaji = prefsRomaji[prefIndex];
                    var prefKanji = prefsKanji[prefIndex];
                    return new CreateData.Pref(
                            prefKanji,
                            patients.get(prefRomaji),
                            hospitalizations.get(prefRomaji),
                            discharges.get(prefRomaji),
                            mortality.get(prefRomaji),
                            severe.get(prefRomaji),
                            0 // TODO PCR検査数をどこからか取得したい。
                    );
                })
                .collect(Collectors.toList());

        PrefJsonProc.writeJson(date, prefs);
    }

    static Map<String, Integer> readCsv(final LocalDate targetDate, final String url, final int columnIndex) throws IOException {
        var result = new HashMap<String, Integer>();
        try (
                var inputStream = new URL(url).openStream();
                var inputStreamReader = new InputStreamReader(inputStream);
                var bufferedReader = new BufferedReader(inputStreamReader)
        ) {
            bufferedReader
                    .lines()
                    .filter(line -> line.startsWith(targetDate.format(DateTimeFormatter.ofPattern("yyyy/M/d"))))
                    .forEach(line -> {
                        var values = line.split(",");
                        result.put(values[1], Integer.valueOf(values[columnIndex + 2]));
                    });
        }
        return result;
    }
}
