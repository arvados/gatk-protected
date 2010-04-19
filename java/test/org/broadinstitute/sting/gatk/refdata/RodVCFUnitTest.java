package org.broadinstitute.sting.gatk.refdata;

import org.broadinstitute.sting.BaseTest;
import org.broadinstitute.sting.utils.GenomeLoc;
import org.broadinstitute.sting.utils.GenomeLocParser;
import org.broadinstitute.sting.utils.StingException;
import org.broadinstitute.sting.utils.fasta.IndexedFastaSequenceFile;
import org.broadinstitute.sting.utils.genotype.vcf.VCFHeader;
import org.broadinstitute.sting.utils.genotype.vcf.VCFReader;
import org.broadinstitute.sting.utils.genotype.vcf.VCFRecord;
import org.broadinstitute.sting.utils.genotype.vcf.VCFGenotypeRecord;
import org.junit.Assert;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * @author aaron
 *         <p/>
 *         Class RodVCFUnitTest
 *         <p/>
 *         test out the rod VCF
 */
public class RodVCFUnitTest extends BaseTest {

    private static File vcfFile = new File(validationDataLocation + "vcfexample.vcf");
    private VCFHeader mHeader;
    @BeforeClass
    public static void beforeTests() {
        IndexedFastaSequenceFile seq;
        try {
            seq = new IndexedFastaSequenceFile(new File(oneKGLocation + "reference/human_b36_both.fasta"));
        } catch (FileNotFoundException e) {
            throw new StingException("unable to load the sequence dictionary");
        }
        GenomeLocParser.setupRefContigOrdering(seq);

    }

    private RodVCF getVCFObject() {
        RodVCF vcf = new RodVCF("VCF");
        mHeader = null;
        try {
            mHeader = (VCFHeader) vcf.initialize(vcfFile);
        } catch (FileNotFoundException e) {
            fail("Unable to open VCF file");
        }
        mHeader.checkVCFVersion();
        return vcf;
    }

    @Test
    public void testInitialize() {
        getVCFObject();
    }

    @Test
    public void testToIterator() {
        RodVCF vcf = getVCFObject();
        VCFReader reader = new VCFReader(vcfFile);
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        while (iter.hasNext()) {
            VCFRecord rec1 = reader.next();
            VCFRecord rec2 = iter.next().mCurrentRecord;
            if (!rec1.equals(rec2)) {
                fail("VCF record rec1 != rec2");
            }
        }
    }

    @Test
    public void testToMAF() {
        RodVCF vcf = getVCFObject();
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        RodVCF rec = iter.next();
        Assert.assertEquals(rec.getNonRefAlleleFrequency(), 0.786, 0.00001);
        rec = iter.next();
        rec = iter.next();
        rec = iter.next();
        Assert.assertEquals(rec.getNonRefAlleleFrequency(), 0.0, 0.00001);
    }

    @Test
    public void testToString() {
        // slightly altered line, due to map ordering
        final String firstLine = "20\t14370\trs6054257\tG\tA\t29.00\t0\tAF=0.786;DP=258;NS=58\tGT:GQ:DP:HQ\t0|0:48:1:51,51\t1|0:48:8:51,51\t1/1:43:5:\n";
        RodVCF vcf = getVCFObject();
        VCFReader reader = new VCFReader(vcfFile);
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        boolean first = true;
        while (iter.hasNext()) {
            VCFRecord rec1 = reader.next();
            VCFRecord rec2 = iter.next().mCurrentRecord;
            if (!rec1.toStringEncoding(mHeader).equals(rec2.toStringEncoding(mHeader))) {
                fail("VCF record rec1.toStringEncoding() != rec2.toStringEncoding()");
            }
            // verify the first line too
            if (first) {
                if (!firstLine.equals(rec1.toStringEncoding(mHeader) + "\n")) {
                    fail("VCF record rec1.toStringEncoding() != expected string :\n" + rec1.toStringEncoding(mHeader) + "\n" + firstLine);
                }
                first = false;
            }
        }
    }

    @Test
    public void testInsertion() {
        RodVCF vcf = getVCFObject();
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        RodVCF rec = iter.next();
        Assert.assertTrue(rec.isSNP());
        rec = iter.next();
        rec = iter.next();
        rec = iter.next();
        rec = iter.next();
        Assert.assertTrue(rec.isIndel());
        Assert.assertTrue(rec.isDeletion());
    }

    @Test
    public void testDeletion() {
        RodVCF vcf = getVCFObject();
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        RodVCF rec = iter.next();
        Assert.assertTrue(rec.isSNP());
        rec = iter.next();
        rec = iter.next();
        rec = iter.next();
        rec = iter.next();
        rec = iter.next();
                
        Assert.assertTrue(rec.isIndel());
        Assert.assertTrue(rec.isInsertion());
    }
    @Test
    public void testGetGenotypes() {        
        RodVCF vcf = getVCFObject();
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        RodVCF rec = iter.next();
        // we should get back a ref 'G' and an alt 'A'
        List<VCFGenotypeRecord> list = rec.getVCFGenotypeRecords();
        List<String> knowngenotypes = new ArrayList<String>();
        knowngenotypes.add("GG");
        knowngenotypes.add("AG");
        knowngenotypes.add("AA");
        Assert.assertEquals(3, list.size());
        for (VCFGenotypeRecord g: list) {
            Assert.assertTrue(knowngenotypes.contains(g.getBases()));
        }
    }
    @Test
    public void testGetGenotypesQual() {
        RodVCF vcf = getVCFObject();
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        RodVCF rec = iter.next();
        // we should get back a ref 'G' and an alt 'A'
        List<VCFGenotypeRecord> list = rec.getVCFGenotypeRecords();
        Assert.assertEquals(4.8,list.get(0).getNegLog10PError(),0.0001);
        Assert.assertEquals(4.8,list.get(1).getNegLog10PError(),0.0001);
        Assert.assertEquals(4.3,list.get(2).getNegLog10PError(),0.0001);
    }
    @Test
    public void testGetLocation() {
        RodVCF vcf = getVCFObject();
        Iterator<RodVCF> iter = vcf.createIterator("VCF", vcfFile);
        RodVCF rec = iter.next();
        GenomeLoc loc = rec.getLocation();
        Assert.assertEquals(14370,loc.getStart());
        Assert.assertEquals(14370,loc.getStop());
        Assert.assertTrue(loc.getContig().equals("20"));
    }

}