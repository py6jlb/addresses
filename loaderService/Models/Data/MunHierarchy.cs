namespace loaderService;

public class MunHierarchy
{ 
    public int Id { get; set; } 
	public int ObjectId { get; set; } 
	public int ParentObjId { get; set; } 
	public int ChangeId { get; set; } 
	public string OKTMO { get; set; } 
	public int PrevId { get; set; } 
	public int NextId { get; set; } 
	public DateTime UpdateDate { get; set; } 
	public DateTime StartDate { get; set; } 
	public DateTime EndDate { get; set; } 
	public int IsActive { get; set; } 
	public string Path { get; set; } 

}
