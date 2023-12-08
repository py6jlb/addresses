namespace loaderService;

public class ChangeHistory
{
    public int ChangeId { get; set; } 
	public int ObjectId { get; set; } 
	public string AdrObjectId { get; set; } 
	public int OperTypeId { get; set; } 
	public DateTime ChangeDate { get; set; }
}
