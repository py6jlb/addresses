namespace loaderService;

public class NormativeDoc
{
    public int Id { get; set; } 
	public string Name { get; set; } 
	public DateTime? Date { get; set; } 
	public string Number { get; set; } 
	public int? Type { get; set; } 
	public int? Kind { get; set; } 
	public DateTime? UpdateDate { get; set; }
	public string OrgName { get; set; } 
	public string RegNum { get; set; } 
	public DateTime? RegDate { get; set; }
	public DateTime? AccDate { get; set; }
	public string Comment { get; set; } 
}
