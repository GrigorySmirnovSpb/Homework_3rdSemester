namespace test;

[TestClass]
public class MovieTests
{
    [TestMethod]
    public void CalculateSimilarity_SameMovie_ReturnsOne()
    {
        var movie = new Movie
        {
            Title = "Test Movie",
            Rating = 8.5,
            Actors = new List<Person> { new Person { Name = "Actor1" } },
            Tags = new List<Tag> { new Tag { Name = "Action" } }
        };
        var movie2 = movie;
        double similarity = movie.CalculateSimilarity(movie2);

        Assert.AreEqual(1.0, similarity);
    }

    [TestMethod]
    public void CalculateSimilarity_SameRating_ReturnsHighRatingScore()
    {
        var movie1 = new Movie
        {
            Title = "Movie 1",
            Rating = 8.0,
            Actors = new List<Person>(),
            Tags = new List<Tag>()
        };

        var movie2 = new Movie
        {
            Title = "Movie 2",
            Rating = 8.0,
            Actors = new List<Person>(),
            Tags = new List<Tag>()
        };

        double similarity = movie1.CalculateSimilarity(movie2);

        Assert.IsGreaterThan(0, similarity);
        Assert.AreEqual(0.5, similarity);
    }

    [TestMethod]
    public void CalculateSimilarity_DifferentRating_ReturnsLowerScore()
    {
        var movie1 = new Movie
        {
            Title = "Movie 1",
            Rating = 10.0,
            Actors = new List<Person>(),
            Tags = new List<Tag>()
        };

        var movie2 = new Movie
        {
            Title = "Movie 2",
            Rating = 0.0,
            Actors = new List<Person>(),
            Tags = new List<Tag>()
        };

        double similarity = movie1.CalculateSimilarity(movie2);
        Assert.AreEqual(0, similarity);
    }

    [TestMethod]
    public void CalculateSimilarity_NullMovie_ReturnsZero()
    {
        var movie = new Movie
        {
            Title = "Test Movie",
            Rating = 8.5
        };

        double similarity = movie.CalculateSimilarity(null!);

        Assert.AreEqual(0, similarity);
    }

    [TestMethod]
    public void CalculateSimilarity_CommonActors_ReturnsActorScore()
    {
        var actor1 = new Person { Name = "John Doe" };
        
        var movie1 = new Movie
        {
            Title = "Movie 1",
            Rating = 7.0,
            Actors = new List<Person> { actor1 },
            Tags = new List<Tag>()
        };

        var movie2 = new Movie
        {
            Title = "Movie 2",
            Rating = 7.0,
            Actors = new List<Person> { actor1 },
            Tags = new List<Tag>()
        };

        double similarity = movie1.CalculateSimilarity(movie2);

        Assert.IsGreaterThanOrEqualTo(0.25, similarity);
    }

    [TestMethod]
    public void CalculateSimilarity_CommonTags_ReturnsTagScore()
    {
        var movie1 = new Movie
        {
            Title = "Movie 1",
            Rating = 7.0,
            Actors = new List<Person>(),
            Tags = new List<Tag> { new Tag { Name = "Action" }, new Tag { Name = "Sci-Fi" } }
        };

        var movie2 = new Movie
        {
            Title = "Movie 2",
            Rating = 7.0,
            Actors = new List<Person>(),
            Tags = new List<Tag> { new Tag { Name = "Action" }, new Tag { Name = "Drama" } }
        };

        double similarity = movie1.CalculateSimilarity(movie2);

        Assert.IsGreaterThanOrEqualTo(0.25, similarity);
    }

    [TestMethod]
    public void CalculateSimilarity_CommonDirector_ReturnsActorScore()
    {
        var director = new Person { Name = "Christopher Nolan" };
        
        var movie1 = new Movie
        {
            Title = "Movie 1",
            Rating = 8.0,
            Director = director,
            Actors = new List<Person>(),
            Tags = new List<Tag>()
        };

        var movie2 = new Movie
        {
            Title = "Movie 2",
            Rating = 8.0,
            Director = director,
            Actors = new List<Person>(),
            Tags = new List<Tag>()
        };

        double similarity = movie1.CalculateSimilarity(movie2);

        Assert.IsGreaterThanOrEqualTo(0.25, similarity);
    }

    [TestMethod]
    public async Task LoadMovieTitlesAsync_ReturnsNonEmptyDictionary()
    {
        var result = await DataProcessor.LoadMovieTitlesAsync();

        Assert.IsNotNull(result);
        Assert.IsNotEmpty(result);
    }

    [TestMethod]
    public async Task LoadPeopleNamesAsync_ReturnsNonEmptyDictionary()
    {
        var result = await DataProcessor.LoadPeopleNamesAsync();

        Assert.IsNotNull(result);
        Assert.IsNotEmpty(result);
    }
    public async Task LoadLinksAsync_ReturnsNonEmptyDictionary()
    {
        var dictionary = await DataProcessor.LoadMovieTitlesAsync();
        var result = await DataProcessor.LoadImdbMovieLensLinksAsync(dictionary);

        Assert.IsNotNull(result);
        Assert.IsNotEmpty(result);
    }
    public async Task LoadRatingAsync_ReturnsNonEmptyDictionary()
    {
        var dictionary = await DataProcessor.LoadMovieTitlesAsync();
        var result = await DataProcessor.LoadRatingsAsync(dictionary);

        Assert.IsNotNull(result);
        Assert.IsNotEmpty(result);
    }

    [TestMethod]
    public async Task LoadMoviePeopleAsync_ReturnsNonEmptyDictionary()
    {
        var people = await DataProcessor.LoadPeopleNamesAsync();
        var movies = await DataProcessor.LoadMovieTitlesAsync();
        var result = await DataProcessor.LoadMoviePeopleAsync(movies,people);

        Assert.IsNotNull(result);
        Assert.IsNotEmpty(result);
    }
    public async Task LoadTagsAsync_ReturnsNonEmptyDictionary()
    {
        var result = await DataProcessor.LoadTagNamesAsync();

        Assert.IsNotNull(result);
        Assert.IsNotEmpty(result);
    }
    public async Task LoadMovieTagsAsync_ReturnsNonEmptyDictionary()
    {   
        var movies = await DataProcessor.LoadMovieTitlesAsync();
        var accordance_movies = await DataProcessor.LoadImdbMovieLensLinksAsync(movies);
        var tags = await DataProcessor.LoadTagNamesAsync();
        var result = await DataProcessor.LoadMovieTagsAsync(accordance_movies, tags);

        Assert.IsNotNull(result);
        Assert.IsNotEmpty(result);
    }
}
