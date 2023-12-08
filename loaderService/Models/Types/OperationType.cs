namespace loaderService.Models.Types;

public class OperationType
{
    public int Id { get; set; }
    public string Name { get; set; }
    public bool IsActive { get; set; }
    public DateTime UpdateDate { get; set; }
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
}
