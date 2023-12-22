namespace loaderService;

public class ChangeHistory
{
    public long ChangeId { get; set; } 
	public long ObjectId { get; set; } 
	public string AdrObjectId { get; set; } 
	public long OperTypeId { get; set; } 
	public long? NDocId { get; set; } 
	public DateTime ChangeDate { get; set; }
}
