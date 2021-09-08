import java.io.Serializable;
import java.sql.Date;
import java.util.UUID;

public class Transaction implements Serializable {
    private String id;
    private int amt;
    private Date tDate;
    private String buId;
    private String piId;

    public Transaction() {
        this.id = UUID.randomUUID().toString();
        this.amt = 0;
        this.tDate = null;
        this.buId = UUID.randomUUID().toString();
        this.piId = UUID.randomUUID().toString();
    }

    public Transaction(final String id, final int amt, final Date tDate, final String buId, final String piId) {
        this.id = id;
        this.amt = amt;
        this.tDate = tDate;
        this.buId = buId;
        this.piId = piId;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public int getAmt() {
        return amt;
    }

    public void setAmt(final int amt) {
        this.amt = amt;
    }

    public Date getTDate() {
        return tDate;
    }

    public void setTDate(final Date tDate) {
        this.tDate = tDate;
    }

    public String getBuId() {
        return buId;
    }

    public void setBuId(final String buId) {
        this.buId = buId;
    }

    public String getPiId() {
        return piId;
    }

    public void setPiId(final String piId) {
        this.piId = piId;
    }

    @Override
    public String toString() {
        return String.format("Transaction{id=%s, amt=%s, tDate=%s, buId=%s, piId=%s}",
                id, amt, tDate.toString(), buId, piId);
    }
}
