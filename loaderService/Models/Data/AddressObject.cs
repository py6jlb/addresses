namespace loaderService;

public class AddressObject
{
    public int Id { get; set; }
    public int ObjectId { get; set; }
    public string ObjectGuid { get; set; }
    public int ChangeId { get; set; }
    public string Name { get; set; }
    public string TypeName { get; set; }
    public int Level { get; set; }
    public int OperTypeId { get; set; }
    public int PrevId { get; set; }
    public int NextId { get; set; }
    public DateTime UpdateDate { get; set; }
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public int IsActual { get; set; }
    public int IsActive { get; set; }
}
