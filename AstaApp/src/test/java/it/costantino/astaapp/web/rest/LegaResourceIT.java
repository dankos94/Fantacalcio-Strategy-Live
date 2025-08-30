package it.costantino.astaapp.web.rest;

import static it.costantino.astaapp.domain.LegaAsserts.*;
import static it.costantino.astaapp.web.rest.TestUtil.createUpdateProxyForBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.costantino.astaapp.IntegrationTest;
import it.costantino.astaapp.domain.Lega;
import it.costantino.astaapp.repository.LegaRepository;
import it.costantino.astaapp.service.dto.LegaDTO;
import it.costantino.astaapp.service.mapper.LegaMapper;
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
 * Integration tests for the {@link LegaResource} REST controller.
 */
@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
class LegaResourceIT {

    private static final String DEFAULT_NOME = "AAAAAAAAAA";
    private static final String UPDATED_NOME = "BBBBBBBBBB";

    private static final Long DEFAULT_BUDGET = 1L;
    private static final Long UPDATED_BUDGET = 2L;

    private static final String ENTITY_API_URL = "/api/legas";
    private static final String ENTITY_API_URL_ID = ENTITY_API_URL + "/{id}";

    private static Random random = new Random();
    private static AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    @Autowired
    private ObjectMapper om;

    @Autowired
    private LegaRepository legaRepository;

    @Autowired
    private LegaMapper legaMapper;

    @Autowired
    private EntityManager em;

    @Autowired
    private MockMvc restLegaMockMvc;

    private Lega lega;

    private Lega insertedLega;

    /**
     * Create an entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Lega createEntity() {
        return new Lega().nome(DEFAULT_NOME).budget(DEFAULT_BUDGET);
    }

    /**
     * Create an updated entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Lega createUpdatedEntity() {
        return new Lega().nome(UPDATED_NOME).budget(UPDATED_BUDGET);
    }

    @BeforeEach
    void initTest() {
        lega = createEntity();
    }

    @AfterEach
    void cleanup() {
        if (insertedLega != null) {
            legaRepository.delete(insertedLega);
            insertedLega = null;
        }
    }

    @Test
    @Transactional
    void createLega() throws Exception {
        long databaseSizeBeforeCreate = getRepositoryCount();
        // Create the Lega
        LegaDTO legaDTO = legaMapper.toDto(lega);
        var returnedLegaDTO = om.readValue(
            restLegaMockMvc
                .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(legaDTO)))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString(),
            LegaDTO.class
        );

        // Validate the Lega in the database
        assertIncrementedRepositoryCount(databaseSizeBeforeCreate);
        var returnedLega = legaMapper.toEntity(returnedLegaDTO);
        assertLegaUpdatableFieldsEquals(returnedLega, getPersistedLega(returnedLega));

        insertedLega = returnedLega;
    }

    @Test
    @Transactional
    void createLegaWithExistingId() throws Exception {
        // Create the Lega with an existing ID
        lega.setId(1L);
        LegaDTO legaDTO = legaMapper.toDto(lega);

        long databaseSizeBeforeCreate = getRepositoryCount();

        // An entity with an existing ID cannot be created, so this API call must fail
        restLegaMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(legaDTO)))
            .andExpect(status().isBadRequest());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeCreate);
    }

    @Test
    @Transactional
    void checkNomeIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        lega.setNome(null);

        // Create the Lega, which fails.
        LegaDTO legaDTO = legaMapper.toDto(lega);

        restLegaMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(legaDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void checkBudgetIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        lega.setBudget(null);

        // Create the Lega, which fails.
        LegaDTO legaDTO = legaMapper.toDto(lega);

        restLegaMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(legaDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void getAllLegas() throws Exception {
        // Initialize the database
        insertedLega = legaRepository.saveAndFlush(lega);

        // Get all the legaList
        restLegaMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(lega.getId().intValue())))
            .andExpect(jsonPath("$.[*].nome").value(hasItem(DEFAULT_NOME)))
            .andExpect(jsonPath("$.[*].budget").value(hasItem(DEFAULT_BUDGET.intValue())));
    }

    @Test
    @Transactional
    void getLega() throws Exception {
        // Initialize the database
        insertedLega = legaRepository.saveAndFlush(lega);

        // Get the lega
        restLegaMockMvc
            .perform(get(ENTITY_API_URL_ID, lega.getId()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.id").value(lega.getId().intValue()))
            .andExpect(jsonPath("$.nome").value(DEFAULT_NOME))
            .andExpect(jsonPath("$.budget").value(DEFAULT_BUDGET.intValue()));
    }

    @Test
    @Transactional
    void getNonExistingLega() throws Exception {
        // Get the lega
        restLegaMockMvc.perform(get(ENTITY_API_URL_ID, Long.MAX_VALUE)).andExpect(status().isNotFound());
    }

    @Test
    @Transactional
    void putExistingLega() throws Exception {
        // Initialize the database
        insertedLega = legaRepository.saveAndFlush(lega);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the lega
        Lega updatedLega = legaRepository.findById(lega.getId()).orElseThrow();
        // Disconnect from session so that the updates on updatedLega are not directly saved in db
        em.detach(updatedLega);
        updatedLega.nome(UPDATED_NOME).budget(UPDATED_BUDGET);
        LegaDTO legaDTO = legaMapper.toDto(updatedLega);

        restLegaMockMvc
            .perform(
                put(ENTITY_API_URL_ID, legaDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(legaDTO))
            )
            .andExpect(status().isOk());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertPersistedLegaToMatchAllProperties(updatedLega);
    }

    @Test
    @Transactional
    void putNonExistingLega() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        lega.setId(longCount.incrementAndGet());

        // Create the Lega
        LegaDTO legaDTO = legaMapper.toDto(lega);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restLegaMockMvc
            .perform(
                put(ENTITY_API_URL_ID, legaDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(legaDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithIdMismatchLega() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        lega.setId(longCount.incrementAndGet());

        // Create the Lega
        LegaDTO legaDTO = legaMapper.toDto(lega);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restLegaMockMvc
            .perform(
                put(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(legaDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithMissingIdPathParamLega() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        lega.setId(longCount.incrementAndGet());

        // Create the Lega
        LegaDTO legaDTO = legaMapper.toDto(lega);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restLegaMockMvc
            .perform(put(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(legaDTO)))
            .andExpect(status().isMethodNotAllowed());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void partialUpdateLegaWithPatch() throws Exception {
        // Initialize the database
        insertedLega = legaRepository.saveAndFlush(lega);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the lega using partial update
        Lega partialUpdatedLega = new Lega();
        partialUpdatedLega.setId(lega.getId());

        partialUpdatedLega.budget(UPDATED_BUDGET);

        restLegaMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedLega.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedLega))
            )
            .andExpect(status().isOk());

        // Validate the Lega in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertLegaUpdatableFieldsEquals(createUpdateProxyForBean(partialUpdatedLega, lega), getPersistedLega(lega));
    }

    @Test
    @Transactional
    void fullUpdateLegaWithPatch() throws Exception {
        // Initialize the database
        insertedLega = legaRepository.saveAndFlush(lega);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the lega using partial update
        Lega partialUpdatedLega = new Lega();
        partialUpdatedLega.setId(lega.getId());

        partialUpdatedLega.nome(UPDATED_NOME).budget(UPDATED_BUDGET);

        restLegaMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedLega.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedLega))
            )
            .andExpect(status().isOk());

        // Validate the Lega in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertLegaUpdatableFieldsEquals(partialUpdatedLega, getPersistedLega(partialUpdatedLega));
    }

    @Test
    @Transactional
    void patchNonExistingLega() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        lega.setId(longCount.incrementAndGet());

        // Create the Lega
        LegaDTO legaDTO = legaMapper.toDto(lega);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restLegaMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, legaDTO.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(legaDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithIdMismatchLega() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        lega.setId(longCount.incrementAndGet());

        // Create the Lega
        LegaDTO legaDTO = legaMapper.toDto(lega);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restLegaMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(legaDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithMissingIdPathParamLega() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        lega.setId(longCount.incrementAndGet());

        // Create the Lega
        LegaDTO legaDTO = legaMapper.toDto(lega);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restLegaMockMvc
            .perform(patch(ENTITY_API_URL).with(csrf()).contentType("application/merge-patch+json").content(om.writeValueAsBytes(legaDTO)))
            .andExpect(status().isMethodNotAllowed());

        // Validate the Lega in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void deleteLega() throws Exception {
        // Initialize the database
        insertedLega = legaRepository.saveAndFlush(lega);

        long databaseSizeBeforeDelete = getRepositoryCount();

        // Delete the lega
        restLegaMockMvc
            .perform(delete(ENTITY_API_URL_ID, lega.getId()).with(csrf()).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());

        // Validate the database contains one less item
        assertDecrementedRepositoryCount(databaseSizeBeforeDelete);
    }

    protected long getRepositoryCount() {
        return legaRepository.count();
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

    protected Lega getPersistedLega(Lega lega) {
        return legaRepository.findById(lega.getId()).orElseThrow();
    }

    protected void assertPersistedLegaToMatchAllProperties(Lega expectedLega) {
        assertLegaAllPropertiesEquals(expectedLega, getPersistedLega(expectedLega));
    }

    protected void assertPersistedLegaToMatchUpdatableProperties(Lega expectedLega) {
        assertLegaAllUpdatablePropertiesEquals(expectedLega, getPersistedLega(expectedLega));
    }
}
