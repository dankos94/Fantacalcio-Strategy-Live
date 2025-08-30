package it.costantino.astaapp.web.rest;

import static it.costantino.astaapp.domain.GiocatoreAsserts.*;
import static it.costantino.astaapp.web.rest.TestUtil.createUpdateProxyForBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.costantino.astaapp.IntegrationTest;
import it.costantino.astaapp.domain.Giocatore;
import it.costantino.astaapp.domain.enumeration.Role;
import it.costantino.astaapp.repository.GiocatoreRepository;
import it.costantino.astaapp.service.dto.GiocatoreDTO;
import it.costantino.astaapp.service.mapper.GiocatoreMapper;
import jakarta.persistence.EntityManager;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for the {@link GiocatoreResource} REST controller.
 */
@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
class GiocatoreResourceIT {

    private static final String DEFAULT_PLAYER = "AAAAAAAAAA";
    private static final String UPDATED_PLAYER = "BBBBBBBBBB";

    private static final String DEFAULT_TEAM = "AAAAAAAAAA";
    private static final String UPDATED_TEAM = "BBBBBBBBBB";

    private static final String DEFAULT_SEASON = "AAAAAAAAAA";
    private static final String UPDATED_SEASON = "BBBBBBBBBB";

    private static final String DEFAULT_AGE = "AAAAAAAAAA";
    private static final String UPDATED_AGE = "BBBBBBBBBB";

    private static final String DEFAULT_SQUAD = "AAAAAAAAAA";
    private static final String UPDATED_SQUAD = "BBBBBBBBBB";

    private static final String DEFAULT_COMP = "AAAAAAAAAA";
    private static final String UPDATED_COMP = "BBBBBBBBBB";

    private static final String DEFAULT_COUNTRY = "AAAAAAAAAA";
    private static final String UPDATED_COUNTRY = "BBBBBBBBBB";

    private static final Long DEFAULT_MP = 1L;
    private static final Long UPDATED_MP = 2L;

    private static final Long DEFAULT_STARTS = 1L;
    private static final Long UPDATED_STARTS = 2L;

    private static final Long DEFAULT_STARTS_PCT = 1L;
    private static final Long UPDATED_STARTS_PCT = 2L;

    private static final Long DEFAULT_MINUTES = 1L;
    private static final Long UPDATED_MINUTES = 2L;

    private static final Long DEFAULT_NINS = 1L;
    private static final Long UPDATED_NINS = 2L;

    private static final Long DEFAULT_GLS = 1L;
    private static final Long UPDATED_GLS = 2L;

    private static final Long DEFAULT_AST = 1L;
    private static final Long UPDATED_AST = 2L;

    private static final Long DEFAULT_G_PLUS_A = 1L;
    private static final Long UPDATED_G_PLUS_A = 2L;

    private static final Long DEFAULT_G_PK = 1L;
    private static final Long UPDATED_G_PK = 2L;

    private static final Long DEFAULT_PK = 1L;
    private static final Long UPDATED_PK = 2L;

    private static final Long DEFAULT_PKATT = 1L;
    private static final Long UPDATED_PKATT = 2L;

    private static final Long DEFAULT_CRDY = 1L;
    private static final Long UPDATED_CRDY = 2L;

    private static final Long DEFAULT_CRDR = 1L;
    private static final Long UPDATED_CRDR = 2L;

    private static final Long DEFAULT_XG = 1L;
    private static final Long UPDATED_XG = 2L;

    private static final Long DEFAULT_XAG = 1L;
    private static final Long UPDATED_XAG = 2L;

    private static final Long DEFAULT_NPXG = 1L;
    private static final Long UPDATED_NPXG = 2L;

    private static final Long DEFAULT_XG_PLUS_XAG = 1L;
    private static final Long UPDATED_XG_PLUS_XAG = 2L;

    private static final Long DEFAULT_NPXG_PLUS_XAG = 1L;
    private static final Long UPDATED_NPXG_PLUS_XAG = 2L;

    private static final Long DEFAULT_PRGP = 1L;
    private static final Long UPDATED_PRGP = 2L;

    private static final Long DEFAULT_PRGC = 1L;
    private static final Long UPDATED_PRGC = 2L;

    private static final Long DEFAULT_PRGR = 1L;
    private static final Long UPDATED_PRGR = 2L;

    private static final Long DEFAULT_GLS_PER_90 = 1L;
    private static final Long UPDATED_GLS_PER_90 = 2L;

    private static final Long DEFAULT_AST_PER_90 = 1L;
    private static final Long UPDATED_AST_PER_90 = 2L;

    private static final Long DEFAULT_G_PLUS_A_PER_90 = 1L;
    private static final Long UPDATED_G_PLUS_A_PER_90 = 2L;

    private static final Long DEFAULT_XG_PER_90 = 1L;
    private static final Long UPDATED_XG_PER_90 = 2L;

    private static final Long DEFAULT_XAG_PER_90 = 1L;
    private static final Long UPDATED_XAG_PER_90 = 2L;

    private static final Long DEFAULT_XG_PLUS_XAG_PER_90 = 1L;
    private static final Long UPDATED_XG_PLUS_XAG_PER_90 = 2L;

    private static final Long DEFAULT_XG_DIFF = 1L;
    private static final Long UPDATED_XG_DIFF = 2L;

    private static final Long DEFAULT_XA_DIFF = 1L;
    private static final Long UPDATED_XA_DIFF = 2L;

    private static final String DEFAULT_TREND = "AAAAAAAAAA";
    private static final String UPDATED_TREND = "BBBBBBBBBB";

    private static final String DEFAULT_NEWLEAGUE = "AAAAAAAAAA";
    private static final String UPDATED_NEWLEAGUE = "BBBBBBBBBB";

    private static final String DEFAULT_SOURCEFILE = "AAAAAAAAAA";
    private static final String UPDATED_SOURCEFILE = "BBBBBBBBBB";

    private static final Role DEFAULT_ROLE = Role.GK;
    private static final Role UPDATED_ROLE = Role.DF;

    private static final Long DEFAULT_CAREER_MP = 1L;
    private static final Long UPDATED_CAREER_MP = 2L;

    private static final Long DEFAULT_CAREER_STARTS = 1L;
    private static final Long UPDATED_CAREER_STARTS = 2L;

    private static final Long DEFAULT_CAREER_MIN = 1L;
    private static final Long UPDATED_CAREER_MIN = 2L;

    private static final Long DEFAULT_CAREER_90_S = 1L;
    private static final Long UPDATED_CAREER_90_S = 2L;

    private static final Long DEFAULT_CAREER_GLS = 1L;
    private static final Long UPDATED_CAREER_GLS = 2L;

    private static final Long DEFAULT_CAREER_AST = 1L;
    private static final Long UPDATED_CAREER_AST = 2L;

    private static final Long DEFAULT_CAREER_G_PLUS_A = 1L;
    private static final Long UPDATED_CAREER_G_PLUS_A = 2L;

    private static final Long DEFAULT_CAREER_XG = 1L;
    private static final Long UPDATED_CAREER_XG = 2L;

    private static final Long DEFAULT_CAREER_XAG = 1L;
    private static final Long UPDATED_CAREER_XAG = 2L;

    private static final Long DEFAULT_CAREER_XG_PLUS_XAG = 1L;
    private static final Long UPDATED_CAREER_XG_PLUS_XAG = 2L;

    private static final Long DEFAULT_CAREER_NPXG = 1L;
    private static final Long UPDATED_CAREER_NPXG = 2L;

    private static final Long DEFAULT_CAREER_NPXG_PLUS_XAG = 1L;
    private static final Long UPDATED_CAREER_NPXG_PLUS_XAG = 2L;

    private static final Long DEFAULT_CAREER_GLS_PER_90 = 1L;
    private static final Long UPDATED_CAREER_GLS_PER_90 = 2L;

    private static final Long DEFAULT_CAREER_AST_PER_90 = 1L;
    private static final Long UPDATED_CAREER_AST_PER_90 = 2L;

    private static final Long DEFAULT_CAREER_G_PLUS_A_PER_90 = 1L;
    private static final Long UPDATED_CAREER_G_PLUS_A_PER_90 = 2L;

    private static final Long DEFAULT_CAREER_XG_PER_90 = 1L;
    private static final Long UPDATED_CAREER_XG_PER_90 = 2L;

    private static final Long DEFAULT_CAREER_XAG_PER_90 = 1L;
    private static final Long UPDATED_CAREER_XAG_PER_90 = 2L;

    private static final Long DEFAULT_CAREER_XG_PLUS_XAG_PER_90 = 1L;
    private static final Long UPDATED_CAREER_XG_PLUS_XAG_PER_90 = 2L;

    private static final String ENTITY_API_URL = "/api/giocatores";
    private static final String ENTITY_API_URL_ID = ENTITY_API_URL + "/{id}";

    private static Random random = new Random();
    private static AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    @Autowired
    private ObjectMapper om;

    @Autowired
    private GiocatoreRepository giocatoreRepository;

    @Autowired
    private GiocatoreMapper giocatoreMapper;

    @Autowired
    private EntityManager em;

    @Autowired
    private MockMvc restGiocatoreMockMvc;

    private Giocatore giocatore;

    private Giocatore insertedGiocatore;

    /**
     * Create an entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Giocatore createEntity() {
        return new Giocatore()
            .player(DEFAULT_PLAYER)
            .team(DEFAULT_TEAM)
            .season(DEFAULT_SEASON)
            .age(DEFAULT_AGE)
            .squad(DEFAULT_SQUAD)
            .comp(DEFAULT_COMP)
            .country(DEFAULT_COUNTRY)
            .mp(DEFAULT_MP)
            .starts(DEFAULT_STARTS)
            .startsPct(DEFAULT_STARTS_PCT)
            .minutes(DEFAULT_MINUTES)
            .nins(DEFAULT_NINS)
            .gls(DEFAULT_GLS)
            .ast(DEFAULT_AST)
            .gPlusA(DEFAULT_G_PLUS_A)
            .gPk(DEFAULT_G_PK)
            .pk(DEFAULT_PK)
            .pkatt(DEFAULT_PKATT)
            .crdy(DEFAULT_CRDY)
            .crdr(DEFAULT_CRDR)
            .xg(DEFAULT_XG)
            .xag(DEFAULT_XAG)
            .npxg(DEFAULT_NPXG)
            .xgPlusXag(DEFAULT_XG_PLUS_XAG)
            .npxgPlusXag(DEFAULT_NPXG_PLUS_XAG)
            .prgp(DEFAULT_PRGP)
            .prgc(DEFAULT_PRGC)
            .prgr(DEFAULT_PRGR)
            .glsPer90(DEFAULT_GLS_PER_90)
            .astPer90(DEFAULT_AST_PER_90)
            .gPlusAPer90(DEFAULT_G_PLUS_A_PER_90)
            .xgPer90(DEFAULT_XG_PER_90)
            .xagPer90(DEFAULT_XAG_PER_90)
            .xgPlusXagPer90(DEFAULT_XG_PLUS_XAG_PER_90)
            .xgDiff(DEFAULT_XG_DIFF)
            .xaDiff(DEFAULT_XA_DIFF)
            .trend(DEFAULT_TREND)
            .newleague(DEFAULT_NEWLEAGUE)
            .sourcefile(DEFAULT_SOURCEFILE)
            .role(DEFAULT_ROLE)
            .careerMp(DEFAULT_CAREER_MP)
            .careerStarts(DEFAULT_CAREER_STARTS)
            .careerMin(DEFAULT_CAREER_MIN)
            .career90s(DEFAULT_CAREER_90_S)
            .careerGls(DEFAULT_CAREER_GLS)
            .careerAst(DEFAULT_CAREER_AST)
            .careerGPlusA(DEFAULT_CAREER_G_PLUS_A)
            .careerXg(DEFAULT_CAREER_XG)
            .careerXag(DEFAULT_CAREER_XAG)
            .careerXgPlusXag(DEFAULT_CAREER_XG_PLUS_XAG)
            .careerNpxg(DEFAULT_CAREER_NPXG)
            .careerNpxgPlusXag(DEFAULT_CAREER_NPXG_PLUS_XAG)
            .careerGlsPer90(DEFAULT_CAREER_GLS_PER_90)
            .careerAstPer90(DEFAULT_CAREER_AST_PER_90)
            .careerGPlusAPer90(DEFAULT_CAREER_G_PLUS_A_PER_90)
            .careerXgPer90(DEFAULT_CAREER_XG_PER_90)
            .careerXagPer90(DEFAULT_CAREER_XAG_PER_90)
            .careerXgPlusXagPer90(DEFAULT_CAREER_XG_PLUS_XAG_PER_90);
    }

    /**
     * Create an updated entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Giocatore createUpdatedEntity() {
        return new Giocatore()
            .player(UPDATED_PLAYER)
            .team(UPDATED_TEAM)
            .season(UPDATED_SEASON)
            .age(UPDATED_AGE)
            .squad(UPDATED_SQUAD)
            .comp(UPDATED_COMP)
            .country(UPDATED_COUNTRY)
            .mp(UPDATED_MP)
            .starts(UPDATED_STARTS)
            .startsPct(UPDATED_STARTS_PCT)
            .minutes(UPDATED_MINUTES)
            .nins(UPDATED_NINS)
            .gls(UPDATED_GLS)
            .ast(UPDATED_AST)
            .gPlusA(UPDATED_G_PLUS_A)
            .gPk(UPDATED_G_PK)
            .pk(UPDATED_PK)
            .pkatt(UPDATED_PKATT)
            .crdy(UPDATED_CRDY)
            .crdr(UPDATED_CRDR)
            .xg(UPDATED_XG)
            .xag(UPDATED_XAG)
            .npxg(UPDATED_NPXG)
            .xgPlusXag(UPDATED_XG_PLUS_XAG)
            .npxgPlusXag(UPDATED_NPXG_PLUS_XAG)
            .prgp(UPDATED_PRGP)
            .prgc(UPDATED_PRGC)
            .prgr(UPDATED_PRGR)
            .glsPer90(UPDATED_GLS_PER_90)
            .astPer90(UPDATED_AST_PER_90)
            .gPlusAPer90(UPDATED_G_PLUS_A_PER_90)
            .xgPer90(UPDATED_XG_PER_90)
            .xagPer90(UPDATED_XAG_PER_90)
            .xgPlusXagPer90(UPDATED_XG_PLUS_XAG_PER_90)
            .xgDiff(UPDATED_XG_DIFF)
            .xaDiff(UPDATED_XA_DIFF)
            .trend(UPDATED_TREND)
            .newleague(UPDATED_NEWLEAGUE)
            .sourcefile(UPDATED_SOURCEFILE)
            .role(UPDATED_ROLE)
            .careerMp(UPDATED_CAREER_MP)
            .careerStarts(UPDATED_CAREER_STARTS)
            .careerMin(UPDATED_CAREER_MIN)
            .career90s(UPDATED_CAREER_90_S)
            .careerGls(UPDATED_CAREER_GLS)
            .careerAst(UPDATED_CAREER_AST)
            .careerGPlusA(UPDATED_CAREER_G_PLUS_A)
            .careerXg(UPDATED_CAREER_XG)
            .careerXag(UPDATED_CAREER_XAG)
            .careerXgPlusXag(UPDATED_CAREER_XG_PLUS_XAG)
            .careerNpxg(UPDATED_CAREER_NPXG)
            .careerNpxgPlusXag(UPDATED_CAREER_NPXG_PLUS_XAG)
            .careerGlsPer90(UPDATED_CAREER_GLS_PER_90)
            .careerAstPer90(UPDATED_CAREER_AST_PER_90)
            .careerGPlusAPer90(UPDATED_CAREER_G_PLUS_A_PER_90)
            .careerXgPer90(UPDATED_CAREER_XG_PER_90)
            .careerXagPer90(UPDATED_CAREER_XAG_PER_90)
            .careerXgPlusXagPer90(UPDATED_CAREER_XG_PLUS_XAG_PER_90);
    }

    @BeforeEach
    void initTest() {
        giocatore = createEntity();
    }

    @AfterEach
    void cleanup() {
        if (insertedGiocatore != null) {
            giocatoreRepository.delete(insertedGiocatore);
            insertedGiocatore = null;
        }
    }

    @Test
    @Transactional
    void createGiocatore() throws Exception {
        long databaseSizeBeforeCreate = getRepositoryCount();
        // Create the Giocatore
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);
        var returnedGiocatoreDTO = om.readValue(
            restGiocatoreMockMvc
                .perform(
                    post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(giocatoreDTO))
                )
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString(),
            GiocatoreDTO.class
        );

        // Validate the Giocatore in the database
        assertIncrementedRepositoryCount(databaseSizeBeforeCreate);
        var returnedGiocatore = giocatoreMapper.toEntity(returnedGiocatoreDTO);
        assertGiocatoreUpdatableFieldsEquals(returnedGiocatore, getPersistedGiocatore(returnedGiocatore));

        insertedGiocatore = returnedGiocatore;
    }

    @Test
    @Transactional
    void createGiocatoreWithExistingId() throws Exception {
        // Create the Giocatore with an existing ID
        giocatore.setId(1L);
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);

        long databaseSizeBeforeCreate = getRepositoryCount();

        // An entity with an existing ID cannot be created, so this API call must fail
        restGiocatoreMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(giocatoreDTO)))
            .andExpect(status().isBadRequest());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeCreate);
    }

    @Test
    @Transactional
    void getAllGiocatores() throws Exception {
        // Initialize the database
        insertedGiocatore = giocatoreRepository.saveAndFlush(giocatore);

        // Get all the giocatoreList
        restGiocatoreMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(giocatore.getId().intValue())))
            .andExpect(jsonPath("$.[*].player").value(hasItem(DEFAULT_PLAYER)))
            .andExpect(jsonPath("$.[*].team").value(hasItem(DEFAULT_TEAM)))
            .andExpect(jsonPath("$.[*].season").value(hasItem(DEFAULT_SEASON)))
            .andExpect(jsonPath("$.[*].age").value(hasItem(DEFAULT_AGE)))
            .andExpect(jsonPath("$.[*].squad").value(hasItem(DEFAULT_SQUAD)))
            .andExpect(jsonPath("$.[*].comp").value(hasItem(DEFAULT_COMP)))
            .andExpect(jsonPath("$.[*].country").value(hasItem(DEFAULT_COUNTRY)))
            .andExpect(jsonPath("$.[*].mp").value(hasItem(DEFAULT_MP.intValue())))
            .andExpect(jsonPath("$.[*].starts").value(hasItem(DEFAULT_STARTS.intValue())))
            .andExpect(jsonPath("$.[*].startsPct").value(hasItem(DEFAULT_STARTS_PCT.intValue())))
            .andExpect(jsonPath("$.[*].minutes").value(hasItem(DEFAULT_MINUTES.intValue())))
            .andExpect(jsonPath("$.[*].nins").value(hasItem(DEFAULT_NINS.intValue())))
            .andExpect(jsonPath("$.[*].gls").value(hasItem(DEFAULT_GLS.intValue())))
            .andExpect(jsonPath("$.[*].ast").value(hasItem(DEFAULT_AST.intValue())))
            .andExpect(jsonPath("$.[*].gPlusA").value(hasItem(DEFAULT_G_PLUS_A.intValue())))
            .andExpect(jsonPath("$.[*].gPk").value(hasItem(DEFAULT_G_PK.intValue())))
            .andExpect(jsonPath("$.[*].pk").value(hasItem(DEFAULT_PK.intValue())))
            .andExpect(jsonPath("$.[*].pkatt").value(hasItem(DEFAULT_PKATT.intValue())))
            .andExpect(jsonPath("$.[*].crdy").value(hasItem(DEFAULT_CRDY.intValue())))
            .andExpect(jsonPath("$.[*].crdr").value(hasItem(DEFAULT_CRDR.intValue())))
            .andExpect(jsonPath("$.[*].xg").value(hasItem(DEFAULT_XG.intValue())))
            .andExpect(jsonPath("$.[*].xag").value(hasItem(DEFAULT_XAG.intValue())))
            .andExpect(jsonPath("$.[*].npxg").value(hasItem(DEFAULT_NPXG.intValue())))
            .andExpect(jsonPath("$.[*].xgPlusXag").value(hasItem(DEFAULT_XG_PLUS_XAG.intValue())))
            .andExpect(jsonPath("$.[*].npxgPlusXag").value(hasItem(DEFAULT_NPXG_PLUS_XAG.intValue())))
            .andExpect(jsonPath("$.[*].prgp").value(hasItem(DEFAULT_PRGP.intValue())))
            .andExpect(jsonPath("$.[*].prgc").value(hasItem(DEFAULT_PRGC.intValue())))
            .andExpect(jsonPath("$.[*].prgr").value(hasItem(DEFAULT_PRGR.intValue())))
            .andExpect(jsonPath("$.[*].glsPer90").value(hasItem(DEFAULT_GLS_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].astPer90").value(hasItem(DEFAULT_AST_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].gPlusAPer90").value(hasItem(DEFAULT_G_PLUS_A_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].xgPer90").value(hasItem(DEFAULT_XG_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].xagPer90").value(hasItem(DEFAULT_XAG_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].xgPlusXagPer90").value(hasItem(DEFAULT_XG_PLUS_XAG_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].xgDiff").value(hasItem(DEFAULT_XG_DIFF.intValue())))
            .andExpect(jsonPath("$.[*].xaDiff").value(hasItem(DEFAULT_XA_DIFF.intValue())))
            .andExpect(jsonPath("$.[*].trend").value(hasItem(DEFAULT_TREND)))
            .andExpect(jsonPath("$.[*].newleague").value(hasItem(DEFAULT_NEWLEAGUE)))
            .andExpect(jsonPath("$.[*].sourcefile").value(hasItem(DEFAULT_SOURCEFILE)))
            .andExpect(jsonPath("$.[*].role").value(hasItem(DEFAULT_ROLE.toString())))
            .andExpect(jsonPath("$.[*].careerMp").value(hasItem(DEFAULT_CAREER_MP.intValue())))
            .andExpect(jsonPath("$.[*].careerStarts").value(hasItem(DEFAULT_CAREER_STARTS.intValue())))
            .andExpect(jsonPath("$.[*].careerMin").value(hasItem(DEFAULT_CAREER_MIN.intValue())))
            .andExpect(jsonPath("$.[*].career90s").value(hasItem(DEFAULT_CAREER_90_S.intValue())))
            .andExpect(jsonPath("$.[*].careerGls").value(hasItem(DEFAULT_CAREER_GLS.intValue())))
            .andExpect(jsonPath("$.[*].careerAst").value(hasItem(DEFAULT_CAREER_AST.intValue())))
            .andExpect(jsonPath("$.[*].careerGPlusA").value(hasItem(DEFAULT_CAREER_G_PLUS_A.intValue())))
            .andExpect(jsonPath("$.[*].careerXg").value(hasItem(DEFAULT_CAREER_XG.intValue())))
            .andExpect(jsonPath("$.[*].careerXag").value(hasItem(DEFAULT_CAREER_XAG.intValue())))
            .andExpect(jsonPath("$.[*].careerXgPlusXag").value(hasItem(DEFAULT_CAREER_XG_PLUS_XAG.intValue())))
            .andExpect(jsonPath("$.[*].careerNpxg").value(hasItem(DEFAULT_CAREER_NPXG.intValue())))
            .andExpect(jsonPath("$.[*].careerNpxgPlusXag").value(hasItem(DEFAULT_CAREER_NPXG_PLUS_XAG.intValue())))
            .andExpect(jsonPath("$.[*].careerGlsPer90").value(hasItem(DEFAULT_CAREER_GLS_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].careerAstPer90").value(hasItem(DEFAULT_CAREER_AST_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].careerGPlusAPer90").value(hasItem(DEFAULT_CAREER_G_PLUS_A_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].careerXgPer90").value(hasItem(DEFAULT_CAREER_XG_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].careerXagPer90").value(hasItem(DEFAULT_CAREER_XAG_PER_90.intValue())))
            .andExpect(jsonPath("$.[*].careerXgPlusXagPer90").value(hasItem(DEFAULT_CAREER_XG_PLUS_XAG_PER_90.intValue())));
    }

    @Test
    @Transactional
    void getGiocatore() throws Exception {
        // Initialize the database
        insertedGiocatore = giocatoreRepository.saveAndFlush(giocatore);

        // Get the giocatore
        restGiocatoreMockMvc
            .perform(get(ENTITY_API_URL_ID, giocatore.getId()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.id").value(giocatore.getId().intValue()))
            .andExpect(jsonPath("$.player").value(DEFAULT_PLAYER))
            .andExpect(jsonPath("$.team").value(DEFAULT_TEAM))
            .andExpect(jsonPath("$.season").value(DEFAULT_SEASON))
            .andExpect(jsonPath("$.age").value(DEFAULT_AGE))
            .andExpect(jsonPath("$.squad").value(DEFAULT_SQUAD))
            .andExpect(jsonPath("$.comp").value(DEFAULT_COMP))
            .andExpect(jsonPath("$.country").value(DEFAULT_COUNTRY))
            .andExpect(jsonPath("$.mp").value(DEFAULT_MP.intValue()))
            .andExpect(jsonPath("$.starts").value(DEFAULT_STARTS.intValue()))
            .andExpect(jsonPath("$.startsPct").value(DEFAULT_STARTS_PCT.intValue()))
            .andExpect(jsonPath("$.minutes").value(DEFAULT_MINUTES.intValue()))
            .andExpect(jsonPath("$.nins").value(DEFAULT_NINS.intValue()))
            .andExpect(jsonPath("$.gls").value(DEFAULT_GLS.intValue()))
            .andExpect(jsonPath("$.ast").value(DEFAULT_AST.intValue()))
            .andExpect(jsonPath("$.gPlusA").value(DEFAULT_G_PLUS_A.intValue()))
            .andExpect(jsonPath("$.gPk").value(DEFAULT_G_PK.intValue()))
            .andExpect(jsonPath("$.pk").value(DEFAULT_PK.intValue()))
            .andExpect(jsonPath("$.pkatt").value(DEFAULT_PKATT.intValue()))
            .andExpect(jsonPath("$.crdy").value(DEFAULT_CRDY.intValue()))
            .andExpect(jsonPath("$.crdr").value(DEFAULT_CRDR.intValue()))
            .andExpect(jsonPath("$.xg").value(DEFAULT_XG.intValue()))
            .andExpect(jsonPath("$.xag").value(DEFAULT_XAG.intValue()))
            .andExpect(jsonPath("$.npxg").value(DEFAULT_NPXG.intValue()))
            .andExpect(jsonPath("$.xgPlusXag").value(DEFAULT_XG_PLUS_XAG.intValue()))
            .andExpect(jsonPath("$.npxgPlusXag").value(DEFAULT_NPXG_PLUS_XAG.intValue()))
            .andExpect(jsonPath("$.prgp").value(DEFAULT_PRGP.intValue()))
            .andExpect(jsonPath("$.prgc").value(DEFAULT_PRGC.intValue()))
            .andExpect(jsonPath("$.prgr").value(DEFAULT_PRGR.intValue()))
            .andExpect(jsonPath("$.glsPer90").value(DEFAULT_GLS_PER_90.intValue()))
            .andExpect(jsonPath("$.astPer90").value(DEFAULT_AST_PER_90.intValue()))
            .andExpect(jsonPath("$.gPlusAPer90").value(DEFAULT_G_PLUS_A_PER_90.intValue()))
            .andExpect(jsonPath("$.xgPer90").value(DEFAULT_XG_PER_90.intValue()))
            .andExpect(jsonPath("$.xagPer90").value(DEFAULT_XAG_PER_90.intValue()))
            .andExpect(jsonPath("$.xgPlusXagPer90").value(DEFAULT_XG_PLUS_XAG_PER_90.intValue()))
            .andExpect(jsonPath("$.xgDiff").value(DEFAULT_XG_DIFF.intValue()))
            .andExpect(jsonPath("$.xaDiff").value(DEFAULT_XA_DIFF.intValue()))
            .andExpect(jsonPath("$.trend").value(DEFAULT_TREND))
            .andExpect(jsonPath("$.newleague").value(DEFAULT_NEWLEAGUE))
            .andExpect(jsonPath("$.sourcefile").value(DEFAULT_SOURCEFILE))
            .andExpect(jsonPath("$.role").value(DEFAULT_ROLE.toString()))
            .andExpect(jsonPath("$.careerMp").value(DEFAULT_CAREER_MP.intValue()))
            .andExpect(jsonPath("$.careerStarts").value(DEFAULT_CAREER_STARTS.intValue()))
            .andExpect(jsonPath("$.careerMin").value(DEFAULT_CAREER_MIN.intValue()))
            .andExpect(jsonPath("$.career90s").value(DEFAULT_CAREER_90_S.intValue()))
            .andExpect(jsonPath("$.careerGls").value(DEFAULT_CAREER_GLS.intValue()))
            .andExpect(jsonPath("$.careerAst").value(DEFAULT_CAREER_AST.intValue()))
            .andExpect(jsonPath("$.careerGPlusA").value(DEFAULT_CAREER_G_PLUS_A.intValue()))
            .andExpect(jsonPath("$.careerXg").value(DEFAULT_CAREER_XG.intValue()))
            .andExpect(jsonPath("$.careerXag").value(DEFAULT_CAREER_XAG.intValue()))
            .andExpect(jsonPath("$.careerXgPlusXag").value(DEFAULT_CAREER_XG_PLUS_XAG.intValue()))
            .andExpect(jsonPath("$.careerNpxg").value(DEFAULT_CAREER_NPXG.intValue()))
            .andExpect(jsonPath("$.careerNpxgPlusXag").value(DEFAULT_CAREER_NPXG_PLUS_XAG.intValue()))
            .andExpect(jsonPath("$.careerGlsPer90").value(DEFAULT_CAREER_GLS_PER_90.intValue()))
            .andExpect(jsonPath("$.careerAstPer90").value(DEFAULT_CAREER_AST_PER_90.intValue()))
            .andExpect(jsonPath("$.careerGPlusAPer90").value(DEFAULT_CAREER_G_PLUS_A_PER_90.intValue()))
            .andExpect(jsonPath("$.careerXgPer90").value(DEFAULT_CAREER_XG_PER_90.intValue()))
            .andExpect(jsonPath("$.careerXagPer90").value(DEFAULT_CAREER_XAG_PER_90.intValue()))
            .andExpect(jsonPath("$.careerXgPlusXagPer90").value(DEFAULT_CAREER_XG_PLUS_XAG_PER_90.intValue()));
    }

    @Test
    @Transactional
    void getNonExistingGiocatore() throws Exception {
        // Get the giocatore
        restGiocatoreMockMvc.perform(get(ENTITY_API_URL_ID, Long.MAX_VALUE)).andExpect(status().isNotFound());
    }

    @Test
    @Transactional
    void putExistingGiocatore() throws Exception {
        // Initialize the database
        insertedGiocatore = giocatoreRepository.saveAndFlush(giocatore);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the giocatore
        Giocatore updatedGiocatore = giocatoreRepository.findById(giocatore.getId()).orElseThrow();
        // Disconnect from session so that the updates on updatedGiocatore are not directly saved in db
        em.detach(updatedGiocatore);
        updatedGiocatore
            .player(UPDATED_PLAYER)
            .team(UPDATED_TEAM)
            .season(UPDATED_SEASON)
            .age(UPDATED_AGE)
            .squad(UPDATED_SQUAD)
            .comp(UPDATED_COMP)
            .country(UPDATED_COUNTRY)
            .mp(UPDATED_MP)
            .starts(UPDATED_STARTS)
            .startsPct(UPDATED_STARTS_PCT)
            .minutes(UPDATED_MINUTES)
            .nins(UPDATED_NINS)
            .gls(UPDATED_GLS)
            .ast(UPDATED_AST)
            .gPlusA(UPDATED_G_PLUS_A)
            .gPk(UPDATED_G_PK)
            .pk(UPDATED_PK)
            .pkatt(UPDATED_PKATT)
            .crdy(UPDATED_CRDY)
            .crdr(UPDATED_CRDR)
            .xg(UPDATED_XG)
            .xag(UPDATED_XAG)
            .npxg(UPDATED_NPXG)
            .xgPlusXag(UPDATED_XG_PLUS_XAG)
            .npxgPlusXag(UPDATED_NPXG_PLUS_XAG)
            .prgp(UPDATED_PRGP)
            .prgc(UPDATED_PRGC)
            .prgr(UPDATED_PRGR)
            .glsPer90(UPDATED_GLS_PER_90)
            .astPer90(UPDATED_AST_PER_90)
            .gPlusAPer90(UPDATED_G_PLUS_A_PER_90)
            .xgPer90(UPDATED_XG_PER_90)
            .xagPer90(UPDATED_XAG_PER_90)
            .xgPlusXagPer90(UPDATED_XG_PLUS_XAG_PER_90)
            .xgDiff(UPDATED_XG_DIFF)
            .xaDiff(UPDATED_XA_DIFF)
            .trend(UPDATED_TREND)
            .newleague(UPDATED_NEWLEAGUE)
            .sourcefile(UPDATED_SOURCEFILE)
            .role(UPDATED_ROLE)
            .careerMp(UPDATED_CAREER_MP)
            .careerStarts(UPDATED_CAREER_STARTS)
            .careerMin(UPDATED_CAREER_MIN)
            .career90s(UPDATED_CAREER_90_S)
            .careerGls(UPDATED_CAREER_GLS)
            .careerAst(UPDATED_CAREER_AST)
            .careerGPlusA(UPDATED_CAREER_G_PLUS_A)
            .careerXg(UPDATED_CAREER_XG)
            .careerXag(UPDATED_CAREER_XAG)
            .careerXgPlusXag(UPDATED_CAREER_XG_PLUS_XAG)
            .careerNpxg(UPDATED_CAREER_NPXG)
            .careerNpxgPlusXag(UPDATED_CAREER_NPXG_PLUS_XAG)
            .careerGlsPer90(UPDATED_CAREER_GLS_PER_90)
            .careerAstPer90(UPDATED_CAREER_AST_PER_90)
            .careerGPlusAPer90(UPDATED_CAREER_G_PLUS_A_PER_90)
            .careerXgPer90(UPDATED_CAREER_XG_PER_90)
            .careerXagPer90(UPDATED_CAREER_XAG_PER_90)
            .careerXgPlusXagPer90(UPDATED_CAREER_XG_PLUS_XAG_PER_90);
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(updatedGiocatore);

        restGiocatoreMockMvc
            .perform(
                put(ENTITY_API_URL_ID, giocatoreDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(giocatoreDTO))
            )
            .andExpect(status().isOk());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertPersistedGiocatoreToMatchAllProperties(updatedGiocatore);
    }

    @Test
    @Transactional
    void putNonExistingGiocatore() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        giocatore.setId(longCount.incrementAndGet());

        // Create the Giocatore
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restGiocatoreMockMvc
            .perform(
                put(ENTITY_API_URL_ID, giocatoreDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(giocatoreDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithIdMismatchGiocatore() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        giocatore.setId(longCount.incrementAndGet());

        // Create the Giocatore
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restGiocatoreMockMvc
            .perform(
                put(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(giocatoreDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithMissingIdPathParamGiocatore() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        giocatore.setId(longCount.incrementAndGet());

        // Create the Giocatore
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restGiocatoreMockMvc
            .perform(put(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(giocatoreDTO)))
            .andExpect(status().isMethodNotAllowed());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void partialUpdateGiocatoreWithPatch() throws Exception {
        // Initialize the database
        insertedGiocatore = giocatoreRepository.saveAndFlush(giocatore);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the giocatore using partial update
        Giocatore partialUpdatedGiocatore = new Giocatore();
        partialUpdatedGiocatore.setId(giocatore.getId());

        partialUpdatedGiocatore
            .player(UPDATED_PLAYER)
            .team(UPDATED_TEAM)
            .season(UPDATED_SEASON)
            .age(UPDATED_AGE)
            .squad(UPDATED_SQUAD)
            .starts(UPDATED_STARTS)
            .startsPct(UPDATED_STARTS_PCT)
            .minutes(UPDATED_MINUTES)
            .nins(UPDATED_NINS)
            .gls(UPDATED_GLS)
            .gPlusA(UPDATED_G_PLUS_A)
            .gPk(UPDATED_G_PK)
            .pkatt(UPDATED_PKATT)
            .crdy(UPDATED_CRDY)
            .crdr(UPDATED_CRDR)
            .npxg(UPDATED_NPXG)
            .prgr(UPDATED_PRGR)
            .astPer90(UPDATED_AST_PER_90)
            .gPlusAPer90(UPDATED_G_PLUS_A_PER_90)
            .xgPer90(UPDATED_XG_PER_90)
            .xagPer90(UPDATED_XAG_PER_90)
            .xgPlusXagPer90(UPDATED_XG_PLUS_XAG_PER_90)
            .xgDiff(UPDATED_XG_DIFF)
            .trend(UPDATED_TREND)
            .role(UPDATED_ROLE)
            .careerStarts(UPDATED_CAREER_STARTS)
            .career90s(UPDATED_CAREER_90_S)
            .careerAst(UPDATED_CAREER_AST)
            .careerXag(UPDATED_CAREER_XAG)
            .careerXgPlusXag(UPDATED_CAREER_XG_PLUS_XAG)
            .careerGlsPer90(UPDATED_CAREER_GLS_PER_90)
            .careerXagPer90(UPDATED_CAREER_XAG_PER_90);

        restGiocatoreMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedGiocatore.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedGiocatore))
            )
            .andExpect(status().isOk());

        // Validate the Giocatore in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertGiocatoreUpdatableFieldsEquals(
            createUpdateProxyForBean(partialUpdatedGiocatore, giocatore),
            getPersistedGiocatore(giocatore)
        );
    }

    @Test
    @Transactional
    void fullUpdateGiocatoreWithPatch() throws Exception {
        // Initialize the database
        insertedGiocatore = giocatoreRepository.saveAndFlush(giocatore);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the giocatore using partial update
        Giocatore partialUpdatedGiocatore = new Giocatore();
        partialUpdatedGiocatore.setId(giocatore.getId());

        partialUpdatedGiocatore
            .player(UPDATED_PLAYER)
            .team(UPDATED_TEAM)
            .season(UPDATED_SEASON)
            .age(UPDATED_AGE)
            .squad(UPDATED_SQUAD)
            .comp(UPDATED_COMP)
            .country(UPDATED_COUNTRY)
            .mp(UPDATED_MP)
            .starts(UPDATED_STARTS)
            .startsPct(UPDATED_STARTS_PCT)
            .minutes(UPDATED_MINUTES)
            .nins(UPDATED_NINS)
            .gls(UPDATED_GLS)
            .ast(UPDATED_AST)
            .gPlusA(UPDATED_G_PLUS_A)
            .gPk(UPDATED_G_PK)
            .pk(UPDATED_PK)
            .pkatt(UPDATED_PKATT)
            .crdy(UPDATED_CRDY)
            .crdr(UPDATED_CRDR)
            .xg(UPDATED_XG)
            .xag(UPDATED_XAG)
            .npxg(UPDATED_NPXG)
            .xgPlusXag(UPDATED_XG_PLUS_XAG)
            .npxgPlusXag(UPDATED_NPXG_PLUS_XAG)
            .prgp(UPDATED_PRGP)
            .prgc(UPDATED_PRGC)
            .prgr(UPDATED_PRGR)
            .glsPer90(UPDATED_GLS_PER_90)
            .astPer90(UPDATED_AST_PER_90)
            .gPlusAPer90(UPDATED_G_PLUS_A_PER_90)
            .xgPer90(UPDATED_XG_PER_90)
            .xagPer90(UPDATED_XAG_PER_90)
            .xgPlusXagPer90(UPDATED_XG_PLUS_XAG_PER_90)
            .xgDiff(UPDATED_XG_DIFF)
            .xaDiff(UPDATED_XA_DIFF)
            .trend(UPDATED_TREND)
            .newleague(UPDATED_NEWLEAGUE)
            .sourcefile(UPDATED_SOURCEFILE)
            .role(UPDATED_ROLE)
            .careerMp(UPDATED_CAREER_MP)
            .careerStarts(UPDATED_CAREER_STARTS)
            .careerMin(UPDATED_CAREER_MIN)
            .career90s(UPDATED_CAREER_90_S)
            .careerGls(UPDATED_CAREER_GLS)
            .careerAst(UPDATED_CAREER_AST)
            .careerGPlusA(UPDATED_CAREER_G_PLUS_A)
            .careerXg(UPDATED_CAREER_XG)
            .careerXag(UPDATED_CAREER_XAG)
            .careerXgPlusXag(UPDATED_CAREER_XG_PLUS_XAG)
            .careerNpxg(UPDATED_CAREER_NPXG)
            .careerNpxgPlusXag(UPDATED_CAREER_NPXG_PLUS_XAG)
            .careerGlsPer90(UPDATED_CAREER_GLS_PER_90)
            .careerAstPer90(UPDATED_CAREER_AST_PER_90)
            .careerGPlusAPer90(UPDATED_CAREER_G_PLUS_A_PER_90)
            .careerXgPer90(UPDATED_CAREER_XG_PER_90)
            .careerXagPer90(UPDATED_CAREER_XAG_PER_90)
            .careerXgPlusXagPer90(UPDATED_CAREER_XG_PLUS_XAG_PER_90);

        restGiocatoreMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedGiocatore.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedGiocatore))
            )
            .andExpect(status().isOk());

        // Validate the Giocatore in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertGiocatoreUpdatableFieldsEquals(partialUpdatedGiocatore, getPersistedGiocatore(partialUpdatedGiocatore));
    }

    @Test
    @Transactional
    void patchNonExistingGiocatore() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        giocatore.setId(longCount.incrementAndGet());

        // Create the Giocatore
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restGiocatoreMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, giocatoreDTO.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(giocatoreDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithIdMismatchGiocatore() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        giocatore.setId(longCount.incrementAndGet());

        // Create the Giocatore
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restGiocatoreMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(giocatoreDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithMissingIdPathParamGiocatore() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        giocatore.setId(longCount.incrementAndGet());

        // Create the Giocatore
        GiocatoreDTO giocatoreDTO = giocatoreMapper.toDto(giocatore);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restGiocatoreMockMvc
            .perform(
                patch(ENTITY_API_URL).with(csrf()).contentType("application/merge-patch+json").content(om.writeValueAsBytes(giocatoreDTO))
            )
            .andExpect(status().isMethodNotAllowed());

        // Validate the Giocatore in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void deleteGiocatore() throws Exception {
        // Initialize the database
        insertedGiocatore = giocatoreRepository.saveAndFlush(giocatore);

        long databaseSizeBeforeDelete = getRepositoryCount();

        // Delete the giocatore
        restGiocatoreMockMvc
            .perform(delete(ENTITY_API_URL_ID, giocatore.getId()).with(csrf()).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());

        // Validate the database contains one less item
        assertDecrementedRepositoryCount(databaseSizeBeforeDelete);
    }

    protected long getRepositoryCount() {
        return giocatoreRepository.count();
    }

    protected void assertIncrementedRepositoryCount(long countBefore) {
        assertThat(countBefore + 1).isEqualTo(getRepositoryCount());
    }

    protected void assertDecrementedRepositoryCount(long countBefore) {
        assertThat(countBefore - 1).isEqualTo(getRepositoryCount());
    }

    protected void assertSameRepositoryCount(long countBefore) {
        assertThat(countBefore).isEqualTo(getRepositoryCount());
    }

    protected Giocatore getPersistedGiocatore(Giocatore giocatore) {
        return giocatoreRepository.findById(giocatore.getId()).orElseThrow();
    }

    protected void assertPersistedGiocatoreToMatchAllProperties(Giocatore expectedGiocatore) {
        assertGiocatoreAllPropertiesEquals(expectedGiocatore, getPersistedGiocatore(expectedGiocatore));
    }

    protected void assertPersistedGiocatoreToMatchUpdatableProperties(Giocatore expectedGiocatore) {
        assertGiocatoreAllUpdatablePropertiesEquals(expectedGiocatore, getPersistedGiocatore(expectedGiocatore));
    }
}
