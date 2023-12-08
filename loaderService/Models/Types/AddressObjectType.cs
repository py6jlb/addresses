namespace loaderService.Models.Types;

public class AddressObjectType
{
    public int Id { get; set; }
    public int Level { get; set; }
    public string Name { get; set; }
    public string ShortName { get; set; }
    public string Desc { get; set; }
    public DateTime UpdateDate { get; set; }
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public bool IsActive { get; set; }
}
