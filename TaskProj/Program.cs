using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

// ============================================
// МОДЕЛИ СУЩНОСТЕЙ
// ============================================

public class Movie
{
    public int Id { get; set; }
    
    public string Title { get; set; }
    
    public string ImdbId { get; set; }
    
    public double Rating { get; set; }
    
    // Режиссер (отношение один-ко-многим)
    public int? DirectorId { get; set; }
    public virtual Person Director { get; set; }
    
    // Актеры (отношение многие-ко-многим)
    public virtual ICollection<Person> Actors { get; set; } = new List<Person>();
    
    // Теги (отношение многие-ко-многим)
    public virtual ICollection<Tag> Tags { get; set; } = new List<Tag>();
    
    // Похожие фильмы (самореференциальное отношение)
    public virtual ICollection<MovieSimilarity> SimilarMovies { get; set; } = new List<MovieSimilarity>();
    
    public double CalculateSimilarity(Movie other)
    {
        if (other == null || this == other) return 0;

        // Оценка по актерам и режиссеру
        double actorScore = 0;
        var allPeople1 = new HashSet<string>(Actors?.Select(a => a.Name) ?? Enumerable.Empty<string>());
        if (Director != null && !string.IsNullOrEmpty(Director.Name)) 
            allPeople1.Add(Director.Name);
        
        var allPeople2 = new HashSet<string>(other.Actors?.Select(a => a.Name) ?? Enumerable.Empty<string>());
        if (other.Director != null && !string.IsNullOrEmpty(other.Director.Name)) 
            allPeople2.Add(other.Director.Name);

        int commonPeople = allPeople1.Intersect(allPeople2).Count();
        int maxPeople = Math.Max(allPeople1.Count, allPeople2.Count);
        if (maxPeople > 0)
        {
            actorScore = 0.25 * (commonPeople / (double)maxPeople);
        }

        // Оценка по тегам
        double tagScore = 0;
        var tags1 = new HashSet<string>(Tags?.Select(t => t.Name) ?? Enumerable.Empty<string>());
        var tags2 = new HashSet<string>(other.Tags?.Select(t => t.Name) ?? Enumerable.Empty<string>());

        int commonTags = tags1.Intersect(tags2).Count();
        int maxTags = Math.Max(tags1.Count, tags2.Count);
        if (maxTags > 0)
        {
            tagScore = 0.25 * (commonTags / (double)maxTags);
        }

        // Оценка по рейтингу
        double ratingScore = 0;
        if (Rating > 0 && other.Rating > 0)
        {
            double diff = Math.Abs(Rating - other.Rating);
            ratingScore = 0.5 * (1.0 - Math.Min(diff / 10.0, 1.0));
        }

        return actorScore + tagScore + ratingScore;
    }
    
    public override string ToString()
    {
        return $"{Title} (Rating: {Rating:F1})\n" +
               $"Director: {(Director != null ? Director.Name : "Unknown")}\n" +
               $"Actors: {string.Join(", ", Actors?.Select(a => a.Name).Take(5) ?? Enumerable.Empty<string>())}\n" +
               $"Tags: {string.Join(", ", Tags?.Select(t => t.Name).Take(5) ?? Enumerable.Empty<string>())}";
    }
}

public class Person
{ 
    public int Id { get; set; } // Убрали DatabaseGeneratedOption.Identity
    public string Name { get; set; }
    
    public virtual ICollection<Movie> MoviesAsDirector { get; set; } = new List<Movie>();
    public virtual ICollection<Movie> MoviesAsActor { get; set; } = new List<Movie>();
    
    public override string ToString() => Name;
}

public class Tag
{
    public int Id { get; set; }
    
    public string Name { get; set; }

    public string TagId { get; set; }
    
    public virtual ICollection<Movie> Movies { get; set; } = new List<Movie>();
    
    public override string ToString() => Name;
}

public class MovieSimilarity
{
    public int SourceMovieId { get; set; }
    public int TargetMovieId { get; set; }
    public double Score { get; set; }
    
    public virtual Movie SourceMovie { get; set; }
    public virtual Movie TargetMovie { get; set; }
    
    public override string ToString()
    {
        return $"{TargetMovie?.Title ?? "Unknown"} (Similarity: {Score:F3}, Rating: {TargetMovie?.Rating ?? 0:F1})";
    }
}

// ============================================
// КОНТЕКСТ БАЗЫ ДАННЫХ (УПРОЩЕННЫЙ)
// ============================================

public class MovieContext : DbContext
{
    public DbSet<Movie> Movies { get; set; }
    public DbSet<Person> People { get; set; }
    public DbSet<Tag> Tags { get; set; }
    public DbSet<MovieSimilarity> MovieSimilarities { get; set; }
    
    // Путь к файлу базы данных
    private static string GetDatabasePath()
    {
        var basePath = AppContext.BaseDirectory;
        var dbPath = Path.Combine(basePath, "movie.db");
        Console.WriteLine($"Путь к БД: {dbPath}");
        return dbPath;
    }
    
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        try
        {
            var dbPath = GetDatabasePath();
            
            // Создаем директорию если не существует
            var directory = Path.GetDirectoryName(dbPath);
            if (!Directory.Exists(directory) && directory != null)
            {
                Directory.CreateDirectory(directory);
            }
            
            // Подключаемся к SQLite
            var connectionString = $"Data Source={dbPath}";
            Console.WriteLine($"Подключение к SQLite: {connectionString}");
            
            optionsBuilder.UseSqlite(connectionString)
                          .LogTo(Console.WriteLine, LogLevel.Warning)
                          .EnableSensitiveDataLogging()
                          .EnableDetailedErrors();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при настройке подключения: {ex.Message}");
            throw;
        }
    }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        
        // Отношение Movie -> Director (Person)
        modelBuilder.Entity<Movie>()
            .HasOne(m => m.Director)
            .WithMany(p => p.MoviesAsDirector)
            .HasForeignKey(m => m.DirectorId)
            .OnDelete(DeleteBehavior.SetNull);
        
        // Отношение Movie <-> Actors (Person) многие-ко-многим
        modelBuilder.Entity<Movie>()
            .HasMany(m => m.Actors)
            .WithMany(p => p.MoviesAsActor)
            .UsingEntity<Dictionary<string, object>>(
                "MovieActor",
                j => j.HasOne<Person>().WithMany().HasForeignKey("PersonId"),
                j => j.HasOne<Movie>().WithMany().HasForeignKey("MovieId")
            );
        
        // Отношение Movie <-> Tags многие-ко-многим
        modelBuilder.Entity<Movie>()
            .HasMany(m => m.Tags)
            .WithMany(t => t.Movies)
            .UsingEntity<Dictionary<string, object>>(
                "MovieTag",
                j => j.HasOne<Tag>().WithMany().HasForeignKey("TagId"),
                j => j.HasOne<Movie>().WithMany().HasForeignKey("MovieId")
            );
        
        // Самореференциальное отношение для похожих фильмов
        modelBuilder.Entity<MovieSimilarity>()
            .HasKey(ms => new { ms.SourceMovieId, ms.TargetMovieId });
        
        modelBuilder.Entity<MovieSimilarity>()
            .HasOne(ms => ms.SourceMovie)
            .WithMany(m => m.SimilarMovies)
            .HasForeignKey(ms => ms.SourceMovieId)
            .OnDelete(DeleteBehavior.Cascade);
        
        modelBuilder.Entity<MovieSimilarity>()
            .HasOne(ms => ms.TargetMovie)
            .WithMany()
            .HasForeignKey(ms => ms.TargetMovieId)
            .OnDelete(DeleteBehavior.Restrict);
        
        // Индексы
        modelBuilder.Entity<Movie>()
            .HasIndex(m => m.Title);
        
        modelBuilder.Entity<Person>()
            .HasIndex(p => p.Name);
        
        modelBuilder.Entity<Tag>()
            .HasIndex(t => t.Name);

        modelBuilder.Entity<Person>()
            .Property(p => p.Id);
    }
}

// ============================================
// УТИЛИТЫ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ
// ============================================

public static class DatabaseHelper
{
    public static async Task<bool> TestDatabaseConnection()
    {
        Console.WriteLine("\n=== ТЕСТ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ ===");
        
        try
        {
            using var context = new MovieContext();
            
            Console.WriteLine("1. Проверка возможности подключения...");
            var canConnect = await context.Database.CanConnectAsync();
            Console.WriteLine($"   Результат: {(canConnect ? "✅ УСПЕШНО" : "❌ НЕ УДАЛОСЬ")}");
            
            if (canConnect)
            {
                Console.WriteLine("2. Проверка существования таблиц...");
                var tables = new[] { "Movies", "People", "Tags", "MovieSimilarities" };
                
                foreach (var table in tables)
                {
                    try
                    {
                        var count = table switch
                        {
                            "Movies" => await context.Movies.CountAsync(),
                            "People" => await context.People.CountAsync(),
                            "Tags" => await context.Tags.CountAsync(),
                            "MovieSimilarities" => await context.MovieSimilarities.CountAsync(),
                            _ => 0
                        };
                        Console.WriteLine($"   Таблица {table}: {count} записей");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"   Таблица {table}: Ошибка - {ex.Message}");
                    }
                }
                
                // Показываем путь к файлу БД
                var dbPath = new SqliteConnectionStringBuilder(context.Database.GetConnectionString()).DataSource;
                Console.WriteLine($"\nФайл базы данных: {dbPath}");
                
                if (File.Exists(dbPath))
                {
                    var fileInfo = new FileInfo(dbPath);
                    Console.WriteLine($"Размер файла: {fileInfo.Length / 1024} KB");
                    Console.WriteLine($"Дата создания: {fileInfo.CreationTime}");
                }
                else
                {
                    Console.WriteLine("Файл БД не существует. Будет создан при первом сохранении.");
                }
            }
            else
            {
                Console.WriteLine("\nСОВЕТЫ ПО УСТРАНЕНИЮ ПРОБЛЕМ:");
                Console.WriteLine("1. Убедитесь, что у вас есть пакеты NuGet:");
                Console.WriteLine("   - Microsoft.EntityFrameworkCore.Sqlite");
                Console.WriteLine("   - Microsoft.EntityFrameworkCore.Tools");
                Console.WriteLine("\n2. Для установки выполните в терминале:");
                Console.WriteLine("   dotnet add package Microsoft.EntityFrameworkCore.Sqlite");
                Console.WriteLine("   dotnet add package Microsoft.EntityFrameworkCore.Tools");
                Console.WriteLine("\n3. Попробуйте создать БД вручную:");
                Console.WriteLine("   using var context = new MovieContext();");
                Console.WriteLine("   await context.Database.EnsureCreatedAsync();");
            }
            
            return canConnect;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Ошибка при тестировании подключения: {ex.Message}");
            Console.WriteLine($"StackTrace: {ex.StackTrace}");
            return false;
        }
    }
    public static async Task ClearDataBase()
    {
        try
        {
            using var context = new MovieContext();
            Console.WriteLine("Очистка старых данных...");
            await context.Database.ExecuteSqlRawAsync("DELETE FROM MovieSimilarities");
            await context.Database.ExecuteSqlRawAsync("DELETE FROM MovieActor");
            await context.Database.ExecuteSqlRawAsync("DELETE FROM MovieTag");
            await context.Database.ExecuteSqlRawAsync("DELETE FROM Movies");
            await context.Database.ExecuteSqlRawAsync("DELETE FROM People");
            await context.Database.ExecuteSqlRawAsync("DELETE FROM Tags");
            Console.WriteLine("Очистка завершена");
            Console.WriteLine("\nНажмите Enter для продолжения...");
            Console.ReadLine();
            Console.Clear();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Ошибка при очистке БД: {ex.Message}");
            throw;
        }
    }
    
    public static async Task CreateDatabaseIfNotExists()
    {
        Console.WriteLine("Создание базы данных (если не существует)...");
        
        try
        {
            using var context = new MovieContext();
            
            // Проверяем существование файла БД
            var dbPath = new SqliteConnectionStringBuilder(context.Database.GetConnectionString()).DataSource;
            Console.WriteLine($"Путь к файлу БД: {dbPath}");
            
            if (!File.Exists(dbPath))
            {
                Console.WriteLine("Создание новой базы данных...");
                await context.Database.EnsureDeletedAsync(); // Удаляем если была
                await context.Database.EnsureCreatedAsync();
                Console.WriteLine("✅ База данных создана успешно!");
            }
            else
            {
                Console.WriteLine("✅ База данных уже существует.");
                var fileInfo = new FileInfo(dbPath);
                Console.WriteLine($"Размер: {fileInfo.Length / 1024} KB");
                
                // Проверяем структуру БД
                try
                {
                    await context.Database.EnsureCreatedAsync();
                    Console.WriteLine("Структура БД проверена.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"⚠️ Предупреждение: {ex.Message}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Ошибка при создании БД: {ex.Message}");
            throw;
        }
    }
}

// ============================================
// ОБРАБОТЧИК ДАННЫХ И ПАРСИНГ
// ============================================

public class DataProcessor
{
    private static Dictionary<string, string> imdbToMovieLens;
    private static Dictionary<string, string> movieTitles;
    private static Dictionary<string, double> ratings;
    private static Dictionary<string, string> peopleNames;
    private static Dictionary<string, List<string>> moviePeople;
    private static Dictionary<string, string> tagNames;
    private static Dictionary<string, List<string>> movieTags;
    private static int batchSize = 1000; // Уменьшили для тестирования
    
    // Временные структуры для парсинга
    private class MovieData
    {
        public string ImdbId { get; set; }
        public string Title { get; set; }
        public double Rating { get; set; }
        public string DirectorName { get; set; }
        public List<string> ActorNames { get; set; } = new List<string>();
        public List<string> TagNames { get; set; } = new List<string>();
    }
    
    public static async Task ParseAndSaveToDatabase()
    {
        Console.WriteLine("=== ПАРСИНГ ФАЙЛОВ И ЗАГРУЗКА В БАЗУ ДАННЫХ ===");
        
        try
        {
            // Сначала создаем БД
            await DatabaseHelper.CreateDatabaseIfNotExists();
            
            Console.WriteLine("1. Загрузка названий фильмов...");
            movieTitles = await LoadMovieTitlesAsync();
            
            Console.WriteLine("2. Загрузка имен актеров и режиссеров...");
            peopleNames = await LoadPeopleNamesAsync();
            
            Console.WriteLine("3. Загрузка соответствий IMDB-MovieLens...");
            imdbToMovieLens = await LoadImdbMovieLensLinksAsync(movieTitles);
            
            Console.WriteLine("4. Загрузка рейтингов...");
            ratings = await LoadRatingsAsync(movieTitles);
            
            Console.WriteLine("5. Загрузка участия людей в фильмах...");
            moviePeople = await LoadMoviePeopleAsync(movieTitles, peopleNames);
            
            Console.WriteLine("6. Загрузка названий тегов...");
            tagNames = await LoadTagNamesAsync();
            
            Console.WriteLine("7. Загрузка тегов фильмов...");
            movieTags = await LoadMovieTagsAsync(imdbToMovieLens, tagNames);
            
            Console.WriteLine("8. Сборка данных и сохранение в БД...");
            await BuildAndSaveToDatabaseAsync();
            
            Console.WriteLine("9. Построение рекомендаций...");
            await BuildSimilaritiesAsync();
            
            Console.WriteLine("\n✅ Все данные успешно загружены в базу данных!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Ошибка при парсинге: {ex.Message}");
            Console.WriteLine($"StackTrace: {ex.StackTrace}");
            
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
            }
        }
    }
    private static async Task BuildAndSaveToDatabaseAsync()
    {
        Console.WriteLine("Начинаем сохранение в БД...");
        
        using var context = new MovieContext();
        
        try
        {
            int processedFilms = 0;
            
            Console.WriteLine("Обработка данных...");
            
            foreach (var movieTitle in movieTitles)
            {
                var imdbId = movieTitle.Key;
                var title = movieTitle.Value;
                
                try
                {
                    if (!ratings.ContainsKey(imdbId))
                        ratings[imdbId] = 0.0;
                    
                    var movie = new Movie
                    {
                        ImdbId = imdbId,
                        Title = title,
                        Rating = ratings[imdbId]
                    };
                    
                    if (moviePeople.TryGetValue(imdbId, out var peopleList))
                    {
                        var directorInfo = peopleList.FirstOrDefault(p => p.StartsWith("director:"));
                        if (directorInfo != null)
                        {
                            var directorName = directorInfo.Substring(9);
                            
                            var director = await context.People
                                .FirstOrDefaultAsync(p => p.Name == directorName);
                            
                            if (director == null)
                            {
                                director = new Person { Name = directorName };
                                context.People.Add(director);
                                await context.SaveChangesAsync();
                            }
                            
                            movie.DirectorId = director.Id;
                        }
                    }
                    
                    context.Movies.Add(movie);
                    await context.SaveChangesAsync();
                    
                    if (moviePeople.TryGetValue(imdbId, out var peopleList2))
                    {
                        var actors = peopleList2
                            .Where(p => p.StartsWith("actor:"))
                            .Select(p => p.Substring(6))
                            .Distinct()
                            .ToList();
                        
                        foreach (var actorName in actors)
                        {
                            var actor = await context.People
                                .FirstOrDefaultAsync(p => p.Name == actorName);
                            
                            if (actor == null)
                            {
                                actor = new Person { Name = actorName };
                                context.People.Add(actor);
                                await context.SaveChangesAsync();
                            }
                            
                            await context.Database.ExecuteSqlRawAsync(
                                "INSERT INTO MovieActor (MovieId, PersonId) VALUES ({0}, {1})",
                                movie.Id, actor.Id);
                        }

                        if (movieTags.TryGetValue(imdbId, out var tagsList))
                        {
                            foreach (var tagName in tagsList.Distinct())
                            {
                                var tagid = tagNames.FirstOrDefault(kvp => kvp.Value == tagName).Key;
                                if (tagid == null)
                                {
                                    tagid = "0";
                                }
                                
                                var tag = await context.Tags
                                    .FirstOrDefaultAsync(t => t.Name == tagName);
                                
                                if (tag == null)
                                {
                                    tag = new Tag { Name = tagName, TagId = tagid };
                                    context.Tags.Add(tag);
                                    await context.SaveChangesAsync();
                                }
                                
                                await context.Database.ExecuteSqlRawAsync(
                                    "INSERT INTO MovieTag (MovieId, TagId) VALUES ({0}, {1})",
                                    movie.Id, tag.Id);
                            }
                        }
                    }
                    
                    processedFilms++;
                    
                    if (processedFilms % 10000 == 0)
                    {
                        Console.WriteLine($"Сохранено {processedFilms} фильмов...");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка при обработке фильма '{title}': {ex.Message}");
                    continue;
                }
            }
            
            Console.WriteLine($"✅ Всего сохранено: {processedFilms} фильмов");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Критическая ошибка при сохранении: {ex.Message}");
            throw;
        }
    }
    
    private static async Task BuildSimilaritiesAsync()
    {
        Console.WriteLine("Построение рекомендаций...");
        
        using var context = new MovieContext();
        
        try
        {
            // Берем только первые 20 фильмов для теста
            var movies = await context.Movies
                .Include(m => m.Actors)
                .Include(m => m.Director)
                .Include(m => m.Tags)
                .ToListAsync();
            
            int processed = 0;
            
            foreach (var movie in movies)
            {
                // Получаем кандидатов для сравнения (только из загруженных фильмов)
                var candidates = movies.Where(m => m.Id != movie.Id).ToList();
                
                var similarities = new List<MovieSimilarity>();
                foreach (var candidate in candidates)
                {
                    double similarity = movie.CalculateSimilarity(candidate);
                    if (similarity > 0.1)
                    {
                        similarities.Add(new MovieSimilarity
                        {
                            SourceMovieId = movie.Id,
                            TargetMovieId = candidate.Id,
                            Score = similarity
                        });
                    }
                }
                
                // Сохраняем топ-3 рекомендаций
                foreach (var similarity in similarities
                    .OrderByDescending(s => s.Score)
                    .Take(3))
                {
                    context.MovieSimilarities.Add(similarity);
                }
                
                processed++;
                if (processed % 5 == 0)
                {
                    await context.SaveChangesAsync();
                    Console.WriteLine($"Обработано {processed}/{movies.Count} фильмов...");
                }
            }
            
            if (context.ChangeTracker.HasChanges())
            {
                await context.SaveChangesAsync();
            }
            
            Console.WriteLine($"✅ Построено рекомендаций для {processed} фильмов");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️ Ошибка при построении рекомендаций: {ex.Message}");
        }
    }
    
    // ============================================
    // МЕТОДЫ ПАРСИНГА ФАЙЛОВ (упрощенные)
    // ============================================
    
    private static Task<Dictionary<string, string>> LoadMovieTitlesAsync()
    {
        return Task.Run(() =>
        {
            var result = new ConcurrentDictionary<string, string>();
            string filePath = "/media/hdd/gregory/CSharp/Tasks/TaskProj/ml-latest/MovieCodes_IMDB.tsv";
            
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Файл {filePath} не найден");
            
            using var reader = new StreamReader(filePath);
            string line;
            bool isFirstLine = true;
            int processed = 0;
            
            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
                    continue;
                }
                
                var parts = line.Split('\t');
                if (parts.Length >= 4 && (parts[3] == "US" || parts[3] == "RU" || parts[4] == "en" || parts[4] == "ru"))
                {
                    var imdbId = parts[0];
                    var title = parts[2];
                    result.TryAdd(imdbId, title);
                }
                
                if (++processed % batchSize == 0)
                {
                    Console.WriteLine($"   Обработано {processed} записей...");
                }
            }
            
            Console.WriteLine($"   Загружено фильмов: {result.Count}");
            return new Dictionary<string, string>(result);
        });
    }
    
    private static Task<Dictionary<string, string>> LoadPeopleNamesAsync()
    {
        return Task.Run(() =>
        {
            var result = new ConcurrentDictionary<string, string>();
            string filePath = "/media/hdd/gregory/CSharp/Tasks/TaskProj/ml-latest/ActorsDirectorsNames_IMDB.txt";
            
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Файл {filePath} не найден");
            
            using var reader = new StreamReader(filePath);
            string line;
            bool isFirstLine = true;
            
            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
                    continue;
                }
                
                var parts = line.Split('\t');
                if (parts.Length >= 2)
                {
                    var personId = parts[0];
                    var name = parts[1];
                    result.TryAdd(personId, name);
                }
            }
            
            Console.WriteLine($"   Загружено людей: {result.Count}");
            return new Dictionary<string, string>(result);
        });
    }
    
    private static Task<Dictionary<string, string>> LoadImdbMovieLensLinksAsync(Dictionary<string, string> trueMovie)
    {
        return Task.Run(() =>
        {
            var result = new ConcurrentDictionary<string, string>();
            string filePath = "/media/hdd/gregory/CSharp/Tasks/TaskProj/ml-latest/links_IMDB_MovieLens.csv";
            
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Файл {filePath} не найден");
            
            using var reader = new StreamReader(filePath);
            string line;
            bool isFirstLine = true;
            
            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
                    continue;
                }
                
                var parts = line.Split(',');
                if (parts.Length >= 2)
                {
                    var movieLensId = parts[0];
                    var imdbId = "tt" + parts[1];
                    if (trueMovie.ContainsKey(imdbId))
                    {
                        result.TryAdd(movieLensId, imdbId);
                    }
                }
            }
            
            Console.WriteLine($"   Загружено соответствий: {result.Count}");
            return new Dictionary<string, string>(result);
        });
    }
    
    private static Task<Dictionary<string, double>> LoadRatingsAsync(Dictionary<string, string> trueMovie)
    {
        return Task.Run(() =>
        {
            var result = new ConcurrentDictionary<string, double>();
            string filePath = "/media/hdd/gregory/CSharp/Tasks/TaskProj/ml-latest/Ratings_IMDB.tsv";
            
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Файл {filePath} не найден");
            
            using var reader = new StreamReader(filePath);
            string line;
            bool isFirstLine = true;
            
            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
                    continue;
                }
                
                var parts = line.Split('\t');
                if (parts.Length >= 2 && double.TryParse(parts[1], out double rating))
                {
                    var imdbId = parts[0];
                    if (trueMovie.ContainsKey(imdbId))
                    {
                        result.TryAdd(imdbId, rating);
                    }
                }
            }
            
            Console.WriteLine($"   Загружено рейтингов: {result.Count}");
            return new Dictionary<string, double>(result);
        });
    }
    
    private static Task<Dictionary<string, List<string>>> LoadMoviePeopleAsync(Dictionary<string, string> trueMovie, Dictionary<string, string> peopleNames)
    {
        return Task.Run(() =>
        {
            var result = new ConcurrentDictionary<string, List<string>>();
            var validPeopleIds = new HashSet<string>(peopleNames.Keys);
            
            string filePath = "/media/hdd/gregory/CSharp/Tasks/TaskProj/ml-latest/ActorsDirectorsCodes_IMDB.tsv";
            
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Файл {filePath} не найден");
            
            using var reader = new StreamReader(filePath);
            string line;
            bool isFirstLine = true;
            int processed = 0;
            
            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
                    continue;
                }
                
                var parts = line.Split('\t');
                if (parts.Length >= 3)
                {
                    var personId = parts[2];
                    
                    if (validPeopleIds.Contains(personId))
                    {
                        var imdbId = parts[0];
                        var category = parts[3];
                        var personName = peopleNames[personId];
                        var personInfo = $"{category}:{personName}";
                        
                        if (trueMovie.ContainsKey(imdbId))
                        {
                            result.AddOrUpdate(imdbId,
                                key => new List<string> { personInfo },
                                (key, existingList) =>
                                {
                                    existingList.Add(personInfo);
                                    return existingList;
                                });
                        }
                    }
                }
                
                if (++processed % batchSize == 0)
                {
                    Console.WriteLine($"   Обработано {processed} записей...");
                }
            }
            
            Console.WriteLine($"   Загружено участий в фильмах: {result.Count}");
            return result.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        });
    }
    
    private static Task<Dictionary<string, string>> LoadTagNamesAsync()
    {
        return Task.Run(() =>
        {
            var result = new ConcurrentDictionary<string, string>();
            string filePath = "/media/hdd/gregory/CSharp/Tasks/TaskProj/ml-latest/TagCodes_MovieLens.csv";
            
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Файл {filePath} не найден");
            
            using var reader = new StreamReader(filePath);
            string line;
            bool isFirstLine = true;
            
            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
                    continue;
                }
                
                var parts = line.Split(',');
                if (parts.Length >= 2)
                {
                    var tagId = parts[0];
                    var tagName = parts[1];
                    result.TryAdd(tagId, tagName);
                }
            }
            
            Console.WriteLine($"   Загружено тегов: {result.Count}");
            return new Dictionary<string, string>(result);
        });
    }
    
    private static Task<Dictionary<string, List<string>>> LoadMovieTagsAsync(
        Dictionary<string, string> imdbToMovieLens,
        Dictionary<string, string> tagNames)
    {
        return Task.Run(() =>
        {
            var result = new ConcurrentDictionary<string, List<string>>();
            
            string filePath = "/media/hdd/gregory/CSharp/Tasks/TaskProj/ml-latest/TagScores_MovieLens.csv";
            
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Файл {filePath} не найден");
            
            using var reader = new StreamReader(filePath);
            string line;
            bool isFirstLine = true;
            
            while ((line = reader.ReadLine()) != null)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
                    continue;
                }
                
                var parts = line.Split(',');
                if (parts.Length >= 3 && 
                    double.TryParse(parts[2], out double score) && 
                    score > 0.5)
                {
                    var movieLensId = parts[0];
                    var tagId = parts[1];
                    
                    if (imdbToMovieLens.TryGetValue(movieLensId, out var imdbId) &&
                        tagNames.TryGetValue(tagId, out var tagName))
                    {
                        result.AddOrUpdate(imdbId,
                            key => new List<string> { tagName },
                            (key, existingList) =>
                            {
                                existingList.Add(tagName);
                                return existingList;
                            });
                    }
                }
            }
            
            Console.WriteLine($"   Загружено тегов фильмов: {result.Count}");
            return result.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        });
    }
}

// ============================================
// КОМАНДЫ БЫСТРОГО ПОИСКА
// ============================================

public static class DatabaseQueries
{
    public static async Task SearchMovieInDatabase()
    {
        Console.Write("Введите название фильма: ");
        var title = Console.ReadLine();
        
        if (string.IsNullOrWhiteSpace(title))
        {
            Console.WriteLine("Пустой запрос.");
            return;
        }
        
        try
        {
            using var context = new MovieContext();
            
            var movies = await context.Movies
                .Include(m => m.Director)
                .Include(m => m.Actors)
                .Include(m => m.Tags)
                .Include(m => m.SimilarMovies)
                    .ThenInclude(ms => ms.TargetMovie)
                .Where(m => m.Title.Contains(title))
                .Take(5)
                .ToListAsync();
            
            if (movies.Any())
            {
                Console.WriteLine($"\nНайдено фильмов: {movies.Count}");
                foreach (var movie in movies)
                {
                    Console.WriteLine($"\n{movie}");
                    
                    if (movie.SimilarMovies.Any())
                    {
                        Console.WriteLine("Похожие фильмы:");
                        foreach (var similar in movie.SimilarMovies
                            .OrderByDescending(s => s.Score)
                            .Take(3))
                        {
                            Console.WriteLine($"  {similar}");
                        }
                    }
                    Console.WriteLine("---");
                }
            }
            else
            {
                Console.WriteLine("Фильмы не найдены.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при поиске: {ex.Message}");
        }
    }
    
    public static async Task SearchPersonInDatabase()
    {
        Console.Write("Введите имя актера или режиссера: ");
        var name = Console.ReadLine();
        
        if (string.IsNullOrWhiteSpace(name))
        {
            Console.WriteLine("Пустой запрос.");
            return;
        }
        
        try
        {
            using var context = new MovieContext();
            
            var people = await context.People
                .Include(p => p.MoviesAsDirector)
                .Include(p => p.MoviesAsActor)
                .Where(p => p.Name.Contains(name))
                .Take(3)
                .ToListAsync();
            
            if (people.Any())
            {
                foreach (var person in people)
                {
                    Console.WriteLine($"\n{person.Name}:");
                    
                    if (person.MoviesAsDirector.Any())
                    {
                        Console.WriteLine($"  Режиссер ({person.MoviesAsDirector.Count} фильмов):");
                        foreach (var movie in person.MoviesAsDirector
                            .OrderByDescending(m => m.Rating)
                            .Take(3))
                        {
                            Console.WriteLine($"    - {movie.Title} (Rating: {movie.Rating:F1})");
                        }
                    }
                    
                    if (person.MoviesAsActor.Any())
                    {
                        Console.WriteLine($"  Актер ({person.MoviesAsActor.Count} фильмов):");
                        foreach (var movie in person.MoviesAsActor
                            .OrderByDescending(m => m.Rating)
                            .Take(3))
                        {
                            Console.WriteLine($"    - {movie.Title} (Rating: {movie.Rating:F1})");
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("Актер/режиссер не найден.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при поиске: {ex.Message}");
        }
    }
    
    public static async Task SearchTagInDatabase()
    {
        Console.Write("Введите тег: ");
        var tag = Console.ReadLine();
        
        if (string.IsNullOrWhiteSpace(tag))
        {
            Console.WriteLine("Пустой запрос.");
            return;
        }
        
        try
        {
            using var context = new MovieContext();
            
            var tags = await context.Tags
                .Include(t => t.Movies)
                    .ThenInclude(m => m.Director)
                .Where(t => t.Name.Contains(tag))
                .Take(3)
                .ToListAsync();
            
            if (tags.Any())
            {
                foreach (var tagObj in tags)
                {
                    Console.WriteLine($"\nФильмы с тегом '{tagObj.Name}': {tagObj.Movies.Count}");
                    foreach (var movie in tagObj.Movies
                        .OrderByDescending(m => m.Rating)
                        .Take(5))
                    {
                        Console.WriteLine($"- {movie.Title} (Rating: {movie.Rating:F1}, Director: {movie.Director?.Name ?? "Unknown"})");
                    }
                }
            }
            else
            {
                Console.WriteLine("Тег не найден.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при поиске: {ex.Message}");
        }
    }
    
    public static async Task ShowStatistics()
    {
        try
        {
            using var context = new MovieContext();
            
            Console.WriteLine("\n=== СТАТИСТИКА БАЗЫ ДАННЫХ ===");
            
            var movieCount = await context.Movies.CountAsync();
            var personCount = await context.People.CountAsync();
            var tagCount = await context.Tags.CountAsync();
            var similarityCount = await context.MovieSimilarities.CountAsync();
            
            Console.WriteLine($"Фильмы: {movieCount}");
            Console.WriteLine($"Люди: {personCount}");
            Console.WriteLine($"Теги: {tagCount}");
            Console.WriteLine($"Рекомендации: {similarityCount}");
            
            var topRated = await context.Movies
                .OrderByDescending(m => m.Rating)
                .Take(5)
                .ToListAsync();
            
            Console.WriteLine("\nТоп-5 фильмов по рейтингу:");
            foreach (var movie in topRated)
            {
                Console.WriteLine($"- {movie.Title}: {movie.Rating:F1}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при получении статистики: {ex.Message}");
        }
    }
}

// ============================================
// ГЛАВНАЯ ПРОГРАММА
// ============================================

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== СИСТЕМА УПРАВЛЕНИЯ ФИЛЬМАМИ ===");
        Console.WriteLine("Упрощенная версия с тестовыми данными");
        
        while (true)
        {
            ShowMenu();
            var choice = Console.ReadLine();
            
            try
            {
                switch (choice)
                {
                    case "1":
                        await DatabaseHelper.TestDatabaseConnection();
                        break;
                    case "2":
                        await DatabaseHelper.CreateDatabaseIfNotExists();
                        break;
                    case "3":
                        await DataProcessor.ParseAndSaveToDatabase();
                        break;
                    case "4":
                        await DatabaseQueries.SearchMovieInDatabase();
                        break;
                    case "5":
                        await DatabaseQueries.SearchPersonInDatabase();
                        break;
                    case "6":
                        await DatabaseQueries.SearchTagInDatabase();
                        break;
                    case "7":
                        await DatabaseQueries.ShowStatistics();
                        break;
                    case "8":
                        await DatabaseHelper.ClearDataBase();
                        break;
                    case "9":
                        Console.WriteLine("Выход из программы.");
                        return;
                    default:
                        Console.WriteLine("Неверный выбор. Попробуйте снова.");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Ошибка: {ex.Message}");
            }
            
            Console.WriteLine("\nНажмите Enter для продолжения...");
            Console.ReadLine();
            Console.Clear();
        }
    }
    
    static void ShowMenu()
    {
        Console.WriteLine("\n=== МЕНЮ ===");
        Console.WriteLine("1. Тестирование подключения к БД");
        Console.WriteLine("2. Создание базы данных");
        Console.WriteLine("3. Загрузка тестовых данных в БД");
        Console.WriteLine("4. Поиск фильма по названию");
        Console.WriteLine("5. Поиск актера/режиссера");
        Console.WriteLine("6. Поиск по тегу");
        Console.WriteLine("7. Показать статистику");
        Console.WriteLine("8. Очистка БД");
        Console.WriteLine("9. Выход");
        Console.Write("Выберите опцию: ");
    }
}