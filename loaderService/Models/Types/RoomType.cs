namespace loaderService.Models.Types;

public class RoomType
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Descr { get; set; }
    public bool IsActive { get; set; }
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public DateTime UpdateDate { get; set; }
}
