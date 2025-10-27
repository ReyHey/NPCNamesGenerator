
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using MySqlConnector;



#pragma warning disable IDE0057

internal static class Program
{
    private static readonly string[] DEFAULT_LOCALES = new[] { "deDE","esES","frFR","itIT","koKR","ptBR","ruRU","zhCN","zhTW" };

    public static async Task<int> Main(string[] args)
    {
        var root = new RootCommand("Rebuilds Locale/NPCNames.lua from TrinityCore DBs");

        var wHost = new Option<string>("--wotlk-host", description: "WotLK/335 DB host") { IsRequired = true };
        var wDb   = new Option<string>("--wotlk-db", description: "WotLK/335 DB name") { IsRequired = true };
        var rHost = new Option<string>("--retail-host", description: "Retail/11.2 DB host") { IsRequired = true };
        var rDb   = new Option<string>("--retail-db", description: "Retail/11.2 DB name") { IsRequired = true };
        var user  = new Option<string>(new[] {"-u","--user"}, description: "MySQL user") { IsRequired = true };
        var pass  = new Option<string>(new[] {"-p","--password"}, description: "MySQL password") { IsRequired = true };
        var wPort = new Option<int>("--wotlk-port", () => 3306, "WotLK/335 DB port");
        var rPort = new Option<int>("--retail-port", () => 3306, "Retail/11.2 DB port");
        var seed  = new Option<string?>("--seed", () => null, "Seed file: JSON list/object or NPCNames.lua");
        var minF  = new Option<int>("--min-fuzzy", () => 86, "Minimum fuzzy score to auto-accept [0-100]");
        var locs  = new Option<string[]>("--locales", () => DEFAULT_LOCALES, "Locales to emit (space separated)");
        var outp  = new Option<string>("--out", () => Path.Combine("Locale","NPCNames.lua"), "Output Lua path");

        root.Add(wHost); root.Add(wDb); root.Add(rHost); root.Add(rDb);
        root.Add(user); root.Add(pass); root.Add(wPort); root.Add(rPort);
        root.Add(seed); root.Add(minF); root.Add(locs); root.Add(outp);

        root.SetHandler(async (wh,wd,rh,rd,u,p,wp,rp,seedPath,minFuzzy,locales,outPath) =>
        {
            await RunAsync(new Config
            {
                WotlkHost = wh, WotlkDb = wd, WotlkPort = wp,
                RetailHost = rh, RetailDb = rd, RetailPort = rp,
                User = u, Password = p, SeedPath = seedPath,
                MinFuzzy = minFuzzy, Locales = locales, OutputPath = outPath
            });
        }, wHost,wDb,rHost,rDb,user,pass,wPort,rPort,seed,minF,locs,outp);

        return await root.InvokeAsync(args);
    }

    private static async Task RunAsync(Config cfg)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(cfg.OutputPath)!);

        Console.WriteLine("Connecting to DBs...");
        var (ct112, loc112) = await LoadDbAsync(cfg.RetailHost, cfg.RetailPort, cfg.RetailDb, cfg.User, cfg.Password);
        var (ct335, loc335) = await LoadDbAsync(cfg.WotlkHost, cfg.WotlkPort, cfg.WotlkDb, cfg.User, cfg.Password);

        var (idToEn, idToLoc) = MergeSources(ct112, loc112, ct335, loc335);

        var enToIds = new Dictionary<string,List<int>>(StringComparer.Ordinal);
        foreach (var kv in idToEn)
        {
            if (!enToIds.TryGetValue(kv.Value, out var list))
            {
                list = new List<int>();
                enToIds[kv.Value] = list;
            }
            list.Add(kv.Key);
        }

        var targets = cfg.SeedPath != null ? LoadSeed(cfg.SeedPath) : idToEn.Values.Distinct().OrderBy(s=>s, StringComparer.Ordinal).ToList();

        // Normalized index
        var normIndex = new Dictionary<string, HashSet<string>>();
        foreach (var en in enToIds.Keys)
        {
            foreach (var key in new[]{ Norm(en), Norm(MaybeSingular(en)) })
            {
                if (!normIndex.TryGetValue(key, out var set))
                {
                    set = new HashSet<string>(StringComparer.Ordinal);
                    normIndex[key] = set;
                }
                set.Add(en);
            }
        }

        var results = new SortedDictionary<string, Dictionary<string,string>>(StringComparer.Ordinal);
        var notFound = new List<string[]>();
        var conflicts = new List<string[]>();

        Console.WriteLine($"Matching {targets.Count} seed names...");
        foreach (var seed in targets)
        {
            var matchedEn = FindBestMatch(seed, enToIds.Keys, normIndex, cfg.MinFuzzy);
            if (matchedEn == null)
            {
                notFound.Add(new[]{ seed, "", "no-match" });
                continue;
            }

            var ids = enToIds[matchedEn];
            int chosen;
            if (ids.Count > 1)
            {
                // pick id with most locales
                chosen = ids.Select(i => (i, CountLocales(idToLoc, i))).OrderByDescending(t=>t.Item2).First().i;
                conflicts.Add(new[]{ seed, matchedEn, $"multiple IDs: {string.Join(',', ids)}" });
            }
            else chosen = ids[0];

            var locMap = new Dictionary<string,string>(StringComparer.Ordinal);
            if (idToLoc.TryGetValue(chosen, out var m))
            {
                foreach (var code in cfg.Locales)
                {
                    if (m.TryGetValue(code, out var val) && !string.IsNullOrWhiteSpace(val))
                        locMap[code] = val;
                }
            }
            if (locMap.Count == 0)
                notFound.Add(new[]{ seed, matchedEn, "no-locales" });

            results[seed] = locMap;
        }

        // Write outputs
        Console.WriteLine("Writing outputs...");
        WriteLua(cfg.OutputPath, results);
        WriteCsv("not_found.csv", new[]{"seed_name","matched_en","reason"}, notFound);
        WriteCsv("conflicts.csv", new[]{"seed_name","matched_en","details"}, conflicts);
        var stats = 
        $"Seeds: {targets.Count}\n" +
        $"Resolved: {results.Count}\n" +
        $"No locales: {notFound.Count(r => r[2] == "no-locales")}\n" +
        $"No match: {notFound.Count(r => r[2] == "no-match")}\n" +
        $"Conflicts: {conflicts.Count}\n";
        File.WriteAllText("stats.txt", stats);


        Console.WriteLine("Done.");
    }

    // --- DB load
    private static async Task<(Dictionary<int,string>, Dictionary<int,Dictionary<string,string>>)> LoadDbAsync(
        string host, int port, string database, string user, string password)
    {
        var cs = new MySqlConnectionStringBuilder
        {
            Server = host, Port = (uint)port, Database = database, UserID = user, Password = password,
            CharacterSet = "utf8mb4", AllowUserVariables = true, ConnectionTimeout = 10
        }.ToString();

        var idToEn = new Dictionary<int,string>();
        var idToLoc = new Dictionary<int,Dictionary<string,string>>();

        await using var conn = new MySqlConnection(cs);
        await conn.OpenAsync();

        // creature_template
        await using (var cmd = new MySqlCommand("SELECT entry AS id, name FROM creature_template", conn))
        await using (var rdr = await cmd.ExecuteReaderAsync())
        {
            while (await rdr.ReadAsync())
            {
                var id = rdr.GetInt32("id");
                var name = rdr.GetString("name");
                idToEn[id] = name;
            }
        }

        // detect locales table
        bool hasModern;
        await using (var cmd = new MySqlCommand("SHOW TABLES LIKE 'creature_template_locale'", conn))
        await using (var rdr = await cmd.ExecuteReaderAsync())
            hasModern = await rdr.ReadAsync();

        if (hasModern)
        {
            const string sql = "SELECT ID AS id, locale, Name AS name FROM creature_template_locale WHERE Name <> ''";
            await using var cmd2 = new MySqlCommand(sql, conn);
            await using var rdr2 = await cmd2.ExecuteReaderAsync();
            while (await rdr2.ReadAsync())
            {
                var id = rdr2.GetInt32("id");
                var locale = rdr2.GetString("locale");
                var name = rdr2.GetString("name");
                if (!idToLoc.TryGetValue(id, out var m)) { m = new(StringComparer.Ordinal); idToLoc[id] = m; }
                m[locale] = name;
            }
        }
        else
        {
            // locales_creature wide-table
            // Discover columns
            var cols = new List<string>();
            await using (var cmd = new MySqlCommand("DESC locales_creature", conn))
            await using (var rdr = await cmd.ExecuteReaderAsync())
            {
                while (await rdr.ReadAsync()) cols.Add(rdr.GetString("Field"));
            }
            var nameCols = cols.Where(c => c.StartsWith("name_loc", StringComparison.OrdinalIgnoreCase)).ToList();
            if (nameCols.Count > 0)
            {
                var sql = $"SELECT entry AS id, {string.Join(", ", nameCols)} FROM locales_creature";
                await using var cmd2 = new MySqlCommand(sql, conn);
                await using var rdr2 = await cmd2.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
                while (await rdr2.ReadAsync())
                {
                    var id = rdr2.GetInt32("id");
                    if (!idToLoc.TryGetValue(id, out var m)) { m = new(StringComparer.Ordinal); idToLoc[id] = m; }
                    foreach (var col in nameCols)
                    {
                        var idx = int.Parse(col.Split('_').Last(), CultureInfo.InvariantCulture);
                        var code = LocIdxToCode(idx);
                        if (code == null) continue;
                        var valObj = rdr2[col];
                        if (valObj != null && valObj != DBNull.Value)
                        {
                            var s = Convert.ToString(valObj, CultureInfo.InvariantCulture);
                            if (!string.IsNullOrWhiteSpace(s)) m[code] = s!;
                        }
                    }
                }
            }
        }

        return (idToEn, idToLoc);
    }

    // --- Merge
    private static (Dictionary<int,string>, Dictionary<int,Dictionary<string,string>>)
        MergeSources(Dictionary<int,string> primaryEn, Dictionary<int,Dictionary<string,string>> primaryLoc,
                     Dictionary<int,string> secondaryEn, Dictionary<int,Dictionary<string,string>> secondaryLoc)
    {
        var ids = new HashSet<int>(primaryEn.Keys); ids.UnionWith(secondaryEn.Keys);
        var idToEn = new Dictionary<int,string>();
        var idToLoc = new Dictionary<int,Dictionary<string,string>>();
        foreach (var id in ids)
        {
            var en = primaryEn.TryGetValue(id, out var p) ? p : (secondaryEn.TryGetValue(id, out var s) ? s : null);
            if (en != null) idToEn[id] = en;

            if (primaryLoc.TryGetValue(id, out var pl)) idToLoc[id] = new(pl);
            if (secondaryLoc.TryGetValue(id, out var sl))
            {
                if (!idToLoc.TryGetValue(id, out var cur)) { cur = new(StringComparer.Ordinal); idToLoc[id] = cur; }
                foreach (var kv in sl) if (!cur.ContainsKey(kv.Key)) cur[kv.Key] = kv.Value;
            }
        }
        return (idToEn, idToLoc);
    }

    private static int CountLocales(Dictionary<int,Dictionary<string,string>> map, int id)
        => map.TryGetValue(id, out var m) ? m.Count : 0;

    // --- Seed
    private static List<string> LoadSeed(string path)
    {
        var ext = Path.GetExtension(path).ToLowerInvariant();
        var text = File.ReadAllText(path, new UTF8Encoding(false));
        if (ext == ".json")
        {
            try
            {
                // List of names
                var list = JsonSerializer.Deserialize<List<string>>(text);
                if (list != null) return list.Where(s=>!string.IsNullOrWhiteSpace(s)).Distinct().OrderBy(s=>s).ToList();
            }
            catch { /* ignore */ }

            // Object mapping { "Name": "tag" }
            var dict = JsonSerializer.Deserialize<Dictionary<string, object>>(text);
            if (dict != null) return dict.Keys.Distinct().OrderBy(s=>s).ToList();

            throw new Exception("Unsupported JSON seed structure");
        }
        if (ext == ".lua")
        {
            var names = new List<string>();
            var rx = new Regex("\\[\\s*\"(.+?)\"\\s*]\\s*=", RegexOptions.Compiled);
            foreach (Match m in rx.Matches(text)) names.Add(m.Groups[1].Value);
            return names.Distinct().OrderBy(s=>s).ToList();
        }
        throw new Exception($"Unsupported seed file: {path}");
    }

    // --- Lua output
    private static void WriteLua(string path, SortedDictionary<string,Dictionary<string,string>> data)
    {
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        w.WriteLine("-- Auto-generated by RebuildNpcNames (C#)\nRXPData = RXPData or {}\nRXPData.NPCNameDB = {");
        foreach (var kv in data)
        {
            var en = LuaEscape(kv.Key);
            var parts = string.Join(", ", kv.Value.OrderBy(p=>p.Key, StringComparer.Ordinal)
                .Select(p => $"{p.Key}=\"{LuaEscape(p.Value)}\""));
            w.WriteLine($"  [\"{en}\"] = {{ {parts} }},");
        }
        w.WriteLine("}");
    }

    private static string LuaEscape(string s) => s.Replace("\\", "\\\\").Replace("\"","\\\"");

    private static void WriteCsv(string path, string[] header, List<string[]> rows)
    {
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        w.WriteLine(string.Join(',', header));
        foreach (var r in rows) w.WriteLine(string.Join(',', r.Select(v => CsvEscape(v))));
    }

    private static string CsvEscape(string v)
    {
        if (v.Contains('"') || v.Contains(',') || v.Contains('\n'))
            return '"' + v.Replace("\"", "\"\"") + '"';
        return v;
    }

    private static string? FindBestMatch(string seed, IEnumerable<string> universe, Dictionary<string,HashSet<string>> normIndex, int minFuzzy)
    {
        if (universe.Contains(seed)) return seed;
        var normSeed = Norm(seed);
        if (normIndex.TryGetValue(normSeed, out var set) && set.Count == 1)
            return set.First();
        var cand = new HashSet<string>(StringComparer.Ordinal);
        if (normIndex.TryGetValue(normSeed, out var s1)) foreach (var s in s1) cand.Add(s);
        var alt = Norm(MaybeSingular(seed));
        if (normIndex.TryGetValue(alt, out var s2)) foreach (var s in s2) cand.Add(s);
        if (cand.Count == 0) cand = new HashSet<string>(universe);

        string? best = null; int bestScore = -1;
        foreach (var c in cand)
        {
            var score = WRatio(seed, c);
            if (score > bestScore) { best = c; bestScore = score; }
        }
        return bestScore >= minFuzzy ? best : null;
    }

    private static int WRatio(string a, string b)
    {
        a = a ?? string.Empty; b = b ?? string.Empty;
        var na = Norm(a); var nb = Norm(b);
        var r1 = Ratio(a,b); var r2 = Ratio(na,nb);
        return Math.Max(r1, r2);
    }

    private static int Ratio(string a, string b)
    {
        int lev = Levenshtein(a, b);
        int maxLen = Math.Max(1, Math.Max(a.Length, b.Length));
        var sim = 1.0 - (double)lev / maxLen;
        return (int)Math.Round(sim * 100);
    }


    private static int Levenshtein(string s, string t)
    {
        var n = s.Length; var m = t.Length;
        var d = new int[n+1, m+1];
        for (int i=0;i<=n;i++) d[i,0]=i;
        for (int j=0;j<=m;j++) d[0,j]=j;
        for (int i=1;i<=n;i++)
        for (int j=1;j<=m;j++)
        {
            int cost = s[i-1]==t[j-1] ? 0 : 1;
            d[i,j] = Math.Min(Math.Min(d[i-1,j]+1, d[i,j-1]+1), d[i-1,j-1]+cost);
        }
        return d[n,m];
    }
 
    // --- Normalization
    private static readonly Regex ParenRx = new("\\s*\\([^)]*\\)", RegexOptions.Compiled);
    private static readonly Regex WsRx = new("\\s+", RegexOptions.Compiled);

    private static string Norm(string s)
    {
        if (string.IsNullOrEmpty(s)) return s;
        s = s.Normalize(NormalizationForm.FormKC);
        s = s.Replace('’','\'').Replace('–','-').Replace('—','-');
        s = ParenRx.Replace(s, "");
        s = WsRx.Replace(s, " ").Trim();
        return s.ToLowerInvariant();
    }

    private static string MaybeSingular(string s)
    {
        var parts = s.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return s;
        var last = parts[^1];
        if (last.Length > 3 && last.EndsWith("s", StringComparison.Ordinal))
            parts[^1] = last[..^1];
        return string.Join(' ', parts);
    }

    // --- Locales mapping for 335 wide table ------------------------------
    private static string? LocIdxToCode(int idx) => idx switch
    {
        1 => "koKR", 2 => "frFR", 3 => "deDE", 4 => "zhCN", 5 => "zhTW",
        6 => "esES", 7 => "esMX", 8 => "ruRU", 9 => "ptBR", 10 => "itIT",
        _ => null
    };
}

internal sealed class Config
{
    public string WotlkHost { get; set; } = "";
    public int WotlkPort { get; set; } = 3306;
    public string WotlkDb   { get; set; } = "";
    public string RetailHost { get; set; } = "";
    public int RetailPort { get; set; } = 3306;
    public string RetailDb   { get; set; } = "";
    public string User { get; set; } = "";
    public string Password { get; set; } = "";
    public string? SeedPath { get; set; }
    public int MinFuzzy { get; set; } = 86;
    public string[] Locales { get; set; } = Array.Empty<string>();
    public string OutputPath { get; set; } = Path.Combine("Locale","NPCNames.lua");
}
