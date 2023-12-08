namespace loaderService.database;

public class GarRepository
{
    private readonly DataContext _context;

    public GarRepository(DataContext context)
    {
        _context = context;
    }
}
