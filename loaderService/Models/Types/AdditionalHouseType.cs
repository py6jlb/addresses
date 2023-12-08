namespace loaderService.Models.Types;

public class HouseType
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string ShortName { get; set; }
    public string Desc { get; set; }
    public bool IsActive { get; set;}
    public DateTime UpdateDate{get;set;}
    public DateTime StartDate{get;set;}
    public DateTime EndDate{get;set;}
}